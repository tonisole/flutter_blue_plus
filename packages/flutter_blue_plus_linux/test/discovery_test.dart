import 'dart:async';

import 'package:bluez/bluez.dart';
import 'package:fake_async/fake_async.dart';
import 'package:flutter_blue_plus/flutter_blue_plus.dart' as fbp;
import 'package:flutter_blue_plus_linux/flutter_blue_plus_linux.dart';
import 'package:flutter_blue_plus_platform_interface/flutter_blue_plus_platform_interface.dart';
import 'package:flutter_test/flutter_test.dart';

const _address = '00:11:22:33:44:55';
final _remoteId = DeviceIdentifier(_address);

void main() {
  test('invocation returns before resolution', () {
    fakeAsync((clock) {
      final bench = _Bench(clock);
      var returned = false;
      bench.platform.discoverServices(bench.request).then((_) => returned = true);
      clock.flushMicrotasks();
      expect(returned, isTrue);
      expect(bench.results, isEmpty);
      bench.resolve();
      expect(bench.results.single.success, isTrue);
      bench.expectClean();
    });
  });

  test('resolved services are serialized and emitted exactly once', () {
    fakeAsync((clock) {
      final bench = _Bench(clock);
      bench.start();
      bench.resolve();
      expect(bench.results.single.success, isTrue);
      expect(bench.results.single.services.single.serviceUuid, Guid('180f'));
      bench.device.events.add(['ServicesResolved']);
      clock.flushMicrotasks();
      clock.elapse(const Duration(seconds: 30));
      expect(bench.results, hasLength(1));
      bench.expectClean();
    });
  });

  test('already resolved discovery cleans up before publishing', () {
    fakeAsync((clock) {
      final bench = _Bench(clock);
      bench.device.resolved = true;
      bench.start();
      expect(bench.results.single.success, isTrue);
      bench.expectClean();
    });
  });

  test('default deadline retires unresolved discovery and ignores late resolution', () {
    fakeAsync((clock) {
      final bench = _Bench(clock);
      bench.start();
      clock.elapse(const Duration(seconds: 14));
      expect(bench.results, isEmpty);
      clock.elapse(const Duration(seconds: 1));
      expect(bench.results.single.success, isFalse);
      expect(bench.results.single.errorString, isNotEmpty);
      bench.expectClean();
      bench.resolve();
      expect(bench.results, hasLength(1));
    });
  });

  for (final timeout in [4, 37]) {
    test('caller-selected $timeout second deadline is not a two-second policy', () {
      fakeAsync((clock) {
        final bench = _Bench(clock);
        bench.platform.discoverServices(BmDiscoverServicesRequest(remoteId: _remoteId, timeout: timeout));
        clock.flushMicrotasks();
        clock.elapse(Duration(seconds: timeout - 1));
        expect(bench.results, isEmpty);
        clock.elapse(const Duration(seconds: 1));
        expect(bench.results.single.success, isFalse);
        bench.expectClean();
        bench.resolve();
        expect(bench.results, hasLength(1));
      });
    });
  }

  for (final edge in ['disconnect', 'remove', 'property error', 'property close', 'removal error', 'removal close']) {
    test('$edge terminates once and releases job resources', () {
      fakeAsync((clock) {
        final bench = _Bench(clock);
        bench.start();
        switch (edge) {
          case 'disconnect':
            bench.device.connectedValue = false;
            bench.device.events.add(['Connected']);
          case 'remove':
            bench.client.currentDevices.clear();
            bench.client.removed.add(bench.device);
          case 'property error':
            bench.device.events.errorLast(StateError('synthetic property failure'));
          case 'property close':
            bench.device.events.closeLast();
          case 'removal error':
            bench.client.removed.errorLast(StateError('synthetic removal failure'));
          case 'removal close':
            bench.client.removed.closeLast();
        }
        clock.flushMicrotasks();
        expect(bench.results.single.success, isFalse);
        expect(bench.results.single.errorString, isNotEmpty);
        // Removal also retires the pre-existing backend logging subscription.
        bench.expectClean(propertyListeners: edge == 'remove' ? 0 : null);
        bench.resolve();
        clock.elapse(const Duration(seconds: 30));
        expect(bench.results, hasLength(1));
      });
    });
  }

  test('removed and re-added address cannot resolve the old session', () {
    fakeAsync((clock) {
      final bench = _Bench(clock);
      bench.start();
      final old = bench.device;
      final replacement = _Device();
      bench.client.currentDevices.clear();
      bench.client.removed.add(old);
      clock.flushMicrotasks();
      bench.client.currentDevices.add(replacement);
      bench.client.added.add(replacement);
      old.resolved = true;
      old.events.add(['ServicesResolved']);
      clock.flushMicrotasks();
      expect(bench.results.single.success, isFalse);
      bench.start();
      old.events.add(['ServicesResolved']);
      clock.flushMicrotasks();
      expect(bench.results, hasLength(1));
      replacement.resolved = true;
      replacement.events.add(['ServicesResolved']);
      clock.flushMicrotasks();
      expect(bench.results.map((r) => r.success), [false, true]);
      expect(old.events.active, 0);
      expect(replacement.events.active, 1); // Existing backend logging subscription.
      expect(clock.nonPeriodicTimerCount, 0);
    });
  });

  test('disconnect and reconnect before queued properties do not reuse old resolution', () {
    fakeAsync((clock) {
      final bench = _Bench(clock);
      bench.start();
      bench.device.connectedValue = false;
      bench.device.events.add(['Connected']);
      bench.device.connectedValue = true;
      bench.device.resolved = true;
      bench.device.events.add(['Connected', 'ServicesResolved']);
      clock.flushMicrotasks();
      expect(bench.results.single.success, isFalse);
      bench.expectClean();
    });
  });

  test('serialization error is reported rather than escaping', () {
    fakeAsync((clock) {
      final bench = _Bench(clock);
      bench.device
        ..resolved = true
        ..servicesError = StateError('synthetic service failure');
      bench.start();
      expect(bench.results.single.success, isFalse);
      expect(bench.results.single.errorString, contains('synthetic service failure'));
      bench.expectClean();
    });
  });

  test('disconnected peer fails without waiting for the deadline', () {
    fakeAsync((clock) {
      final bench = _Bench(clock);
      bench.device.connectedValue = false;
      bench.start();
      expect(bench.results.single.success, isFalse);
      bench.expectClean();
    });
  });

  test('retries do not accumulate listeners or timers', () {
    fakeAsync((clock) {
      final bench = _Bench(clock);
      for (var i = 0; i < 10; i++) {
        bench.device.resolved = false;
        bench.start();
        bench.resolve();
        expect(bench.results, hasLength(i + 1));
        bench.expectClean();
      }
    });
  });

  test('fresh BlueZ device wrappers still belong to the same discovery session', () {
    fakeAsync((clock) {
      final bench = _Bench(clock);
      bench.client.freshWrappers = true;
      bench.start();
      bench.resolve();
      expect(bench.results.single.success, isTrue);
      bench.expectClean();
    });
  });

  test('success waits for both cancellation futures before publishing', () {
    fakeAsync((clock) {
      final bench = _Bench(clock);
      bench.start();
      final propertyCleanup = Completer<void>();
      final removalCleanup = Completer<void>();
      bench.device.events.cancelGate = propertyCleanup;
      bench.client.removed.cancelGate = removalCleanup;
      bench.resolve();
      expect(bench.results, isEmpty);
      expect(clock.nonPeriodicTimerCount, 0);
      propertyCleanup.complete();
      clock.flushMicrotasks();
      expect(bench.results, isEmpty);
      removalCleanup.complete();
      clock.flushMicrotasks();
      expect(bench.results.single.success, isTrue);
      bench.expectClean();
    });
  });

  test('replacement during cleanup cannot publish an old success', () {
    fakeAsync((clock) {
      final bench = _Bench(clock);
      bench.start();
      final cleanup = Completer<void>();
      bench.device.events.cancelGate = cleanup;
      bench.resolve();
      expect(bench.results, isEmpty);
      bench.client.currentDevices
        ..clear()
        ..add(_Device());
      cleanup.complete();
      clock.flushMicrotasks();
      expect(bench.results.single.success, isFalse);
      bench.expectClean();
    });
  });

  test('disconnect during cleanup invalidates success before BlueZ acknowledges it', () {
    fakeAsync((clock) {
      final bench = _Bench(clock);
      bench.start();
      final cleanup = Completer<void>();
      final disconnect = Completer<void>();
      bench.device.events.cancelGate = cleanup;
      bench.device.disconnectGate = disconnect;
      bench.resolve();
      expect(bench.results, isEmpty);
      bench.platform.disconnect(BmDisconnectRequest(remoteId: _remoteId));
      clock.flushMicrotasks();
      expect(bench.device.connected, isTrue);
      cleanup.complete();
      clock.flushMicrotasks();
      expect(bench.results.single.success, isFalse);
      disconnect.complete();
      clock.flushMicrotasks();
      expect(bench.results, hasLength(1));
      bench.expectClean();
    });
  });

  test('cleanup errors become one failure and all cancellations are attempted', () {
    fakeAsync((clock) {
      final bench = _Bench(clock);
      bench.start();
      final cleanup = Completer<void>();
      bench.device.events.cancelGate = cleanup;
      bench.resolve();
      cleanup.completeError(StateError('synthetic cancellation failure'));
      clock.flushMicrotasks();
      expect(bench.results.single.success, isFalse);
      expect(bench.results.single.errorString, contains('synthetic cancellation failure'));
      bench.expectClean();
    });
  });

  test('setup error cleans up partially installed subscriptions', () {
    fakeAsync((clock) {
      final bench = _Bench(clock);
      bench.device.events.listenError = StateError('synthetic subscription failure');
      bench.start();
      expect(bench.results.single.success, isFalse);
      expect(bench.results.single.errorString, contains('synthetic subscription failure'));
      bench.expectClean();
    });
  });

  test('missing device reports a failure without allocating a job', () {
    fakeAsync((clock) {
      final bench = _Bench(clock);
      bench.client.currentDevices.clear();
      bench.start();
      expect(bench.results.single.success, isFalse);
      bench.expectClean();
    });
  });

  test('overlapping direct requests do not create a second job', () {
    fakeAsync((clock) {
      final bench = _Bench(clock);
      bench.start();
      Object? overlapError;
      bench.platform.discoverServices(bench.request).then<void>((_) => fail('overlapping request was accepted'),
          onError: (Object error) {
        overlapError = error;
      });
      clock.flushMicrotasks();
      expect(overlapError, isStateError);
      expect(bench.device.events.active, bench.idleProperties + 1);
      expect(bench.client.removed.active, bench.idleRemovals + 1);
      expect(clock.nonPeriodicTimerCount, 1);
      bench.resolve();
      expect(bench.results.single.success, isTrue);
      bench.expectClean();
    });
  });

  test('initialization error produces one failure without a discovery job', () {
    fakeAsync((clock) {
      final client = _Client(_Device())..connectError = StateError('synthetic initialization failure');
      final platform = FlutterBluePlusLinux(client: client);
      final results = <BmDiscoverServicesResult>[];
      platform.onDiscoveredServices.listen(results.add);
      platform.discoverServices(BmDiscoverServicesRequest(remoteId: _remoteId));
      clock.flushMicrotasks();
      expect(results.single.success, isFalse);
      expect(results.single.errorString, contains('synthetic initialization failure'));
      expect(client.removed.active, 0);
      expect(clock.nonPeriodicTimerCount, 0);
    });
  });

  test('disconnect invocation retires discovery even without a BlueZ property event', () {
    fakeAsync((clock) {
      final bench = _Bench(clock);
      bench.device.emitDisconnect = false;
      bench.start();
      var disconnected = false;
      bench.platform.disconnect(BmDisconnectRequest(remoteId: _remoteId)).then((_) => disconnected = true);
      clock.flushMicrotasks();
      expect(disconnected, isTrue);
      expect(bench.results.single.success, isFalse);
      bench.expectClean();
      bench.resolve();
      expect(bench.results, hasLength(1));
    });
  });

  test('public SDK unrelated invocation and queue:false disconnect are not locked out', () {
    fakeAsync((clock) {
      final bench = _Bench(clock);
      FlutterBluePlusPlatform.instance = bench.platform;
      fbp.FlutterBluePlus.setLogLevel(fbp.LogLevel.none);
      var initialized = false;
      fbp.FlutterBluePlus.isSupported.then((_) => initialized = true);
      clock.flushMicrotasks();
      expect(initialized, isTrue);
      final device = fbp.BluetoothDevice.fromId(_address);
      // Establish the SDK's real connection-state cache through the backend stream.
      bench.device.events.add(['Connected']);
      clock.flushMicrotasks();
      expect(device.isConnected, isTrue);
      Object? discoveryError;
      device.discoverServices(subscribeToServicesChanged: false).then<void>(
        (_) => fail('unresolved discovery unexpectedly succeeded'),
        onError: (Object error) {
          discoveryError = error;
        },
      );
      clock.flushMicrotasks();
      var unrelatedReturned = false;
      fbp.FlutterBluePlus.isSupported.then((_) => unrelatedReturned = true);
      clock.flushMicrotasks();
      expect(unrelatedReturned, isTrue);
      var disconnected = false;
      device.disconnect(queue: false).then((_) => disconnected = true);
      clock.flushMicrotasks();
      expect(bench.device.disconnectCalls, 1);
      expect(disconnected, isTrue);
      expect(discoveryError, isNotNull);
      expect(bench.results.single.success, isFalse);
      bench.device.resolved = true;
      bench.device.events.add(['ServicesResolved']);
      clock.flushMicrotasks();
      expect(bench.results, hasLength(1));
      expect(clock.nonPeriodicTimerCount, 0);
    });
  });
}

class _Bench {
  final FakeAsync clock;
  final device = _Device();
  late final client = _Client(device);
  late final platform = FlutterBluePlusLinux(client: client);
  final results = <BmDiscoverServicesResult>[];
  final request = BmDiscoverServicesRequest(remoteId: _remoteId);
  late final int idleProperties;
  late final int idleRemovals;

  _Bench(this.clock) {
    platform.onDiscoveredServices.listen(results.add);
    platform.isSupported(BmIsSupportedRequest());
    clock.flushMicrotasks();
    idleProperties = device.events.active;
    idleRemovals = client.removed.active;
  }

  void start() {
    platform.discoverServices(request);
    clock.flushMicrotasks();
  }

  void resolve() {
    device.resolved = true;
    device.events.add(['ServicesResolved']);
    clock.flushMicrotasks();
  }

  void expectClean({int? propertyListeners}) {
    expect(device.events.active, propertyListeners ?? idleProperties);
    expect(client.removed.active, idleRemovals);
    expect(clock.nonPeriodicTimerCount, 0);
  }
}

class _Client implements BlueZClient {
  final List<BlueZDevice> currentDevices;
  final added = _Events<BlueZDevice>();
  final removed = _Events<BlueZDevice>();
  bool freshWrappers = false;
  Object? connectError;

  _Client(_Device device) : currentDevices = [device];

  @override
  List<BlueZDevice> get devices =>
      freshWrappers ? currentDevices.cast<_Device>().map(_DeviceView.new).toList() : currentDevices;
  @override
  List<BlueZAdapter> get adapters => [];
  @override
  Stream<BlueZDevice> get deviceAdded => added;
  @override
  Stream<BlueZDevice> get deviceRemoved => removed;
  @override
  Stream<BlueZAdapter> get adapterAdded => const Stream.empty();
  @override
  Stream<BlueZAdapter> get adapterRemoved => const Stream.empty();
  @override
  Future<void> connect() async {
    if (connectError != null) throw connectError!;
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

class _Device implements BlueZDevice {
  final events = _Events<List<String>>();
  bool resolved = false;
  bool connectedValue = true;
  Object? servicesError;
  int disconnectCalls = 0;
  bool emitDisconnect = true;
  Completer<void>? disconnectGate;

  @override
  String get address => _address;
  @override
  bool get connected => connectedValue;
  @override
  bool get servicesResolved => resolved;
  @override
  Stream<List<String>> get propertiesChanged => events;
  @override
  List<BlueZGattService> get gattServices {
    if (servicesError != null) {
      throw servicesError!;
    }
    return [_Service()];
  }

  @override
  Future<void> disconnect() async {
    disconnectCalls++;
    if (disconnectGate != null) await disconnectGate!.future;
    connectedValue = false;
    if (emitDisconnect) events.add(['Connected']);
  }

  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

class _Service implements BlueZGattService {
  @override
  BlueZUUID get uuid => BlueZUUID.fromString('0000180f-0000-1000-8000-00805f9b34fb');
  @override
  List<BlueZGattCharacteristic> get characteristics => [];
  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

// BlueZClient.devices returns new wrappers around the same underlying object.
class _DeviceView implements BlueZDevice {
  final _Device device;
  _DeviceView(this.device);
  @override
  String get address => device.address;
  @override
  bool get connected => device.connected;
  @override
  bool get servicesResolved => device.servicesResolved;
  @override
  Stream<List<String>> get propertiesChanged => device.propertiesChanged;
  @override
  List<BlueZGattService> get gattServices => device.gattServices;
  @override
  dynamic noSuchMethod(Invocation invocation) => super.noSuchMethod(invocation);
}

// Each subscription has its own controller, so dependency errors/closure can be
// injected into the discovery job without changing the existing logging streams.
class _Events<T> extends Stream<T> {
  final _controllers = <StreamController<T>>[];
  int get active => _controllers.length;
  Completer<void>? cancelGate;
  Object? listenError;

  void add(T event) {
    for (final controller in _controllers.toList()) {
      controller.add(event);
    }
  }

  void errorLast(Object error) => _controllers.last.addError(error);
  void closeLast() => _controllers.last.close();

  @override
  StreamSubscription<T> listen(void Function(T)? onData,
      {Function? onError, void Function()? onDone, bool? cancelOnError}) {
    if (listenError != null) {
      throw listenError!;
    }
    late final StreamController<T> controller;
    controller = StreamController<T>(onCancel: () {
      _controllers.remove(controller);
      return cancelGate?.future ?? Future<void>.value();
    });
    _controllers.add(controller);
    return controller.stream.listen(onData, onError: onError, onDone: onDone, cancelOnError: cancelOnError);
  }
}
