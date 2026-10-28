import 'package:bluez/bluez.dart';
import 'package:dbus/dbus.dart';
import 'package:flutter_blue_plus_linux/flutter_blue_plus_linux.dart';
import 'package:flutter_blue_plus_platform_interface/flutter_blue_plus_platform_interface.dart';
import 'package:flutter_test/flutter_test.dart';

const _address = '02:00:00:00:00:01';
const _devicePath = '/org/bluez/hci0/dev_02_00_00_00_00_01';
const _serviceUuid = '0000180f-0000-1000-8000-00805f9b34fb';
const _levelUuid = '00002a19-0000-1000-8000-00805f9b34fb';
const _statusUuid = '00002a1a-0000-1000-8000-00805f9b34fb';
const _deadline = Duration(seconds: 2);

/// Intercepts public D-Bus calls without opening a socket or using a radio.
///
/// BlueZ itself decodes the object tree and recreates its public GATT wrappers.
class _GattBus extends DBusClient {
  final objects = <DBusObjectPath, Map<String, Map<String, DBusValue>>>{};
  final reads = <DBusObjectPath>[];
  final values = <DBusObjectPath, int>{};

  _GattBus(List<List<String>> services) : super(DBusAddress('unix:path=/unused-gatt-test-bus')) {
    objects[DBusObjectPath(_devicePath)] = {
      'org.bluez.Device1': {
        'Address': const DBusString(_address),
        'ServicesResolved': const DBusBoolean(true),
      },
    };
    for (var s = 0; s < services.length; s++) {
      final servicePath = '$_devicePath/service$s';
      objects[DBusObjectPath(servicePath)] = {
        'org.bluez.GattService1': {
          'Device': DBusObjectPath(_devicePath),
          'UUID': const DBusString(_serviceUuid),
          'Primary': const DBusBoolean(true),
        },
      };
      for (var c = 0; c < services[s].length; c++) {
        final path = DBusObjectPath('$servicePath/char$c');
        values[path] = values.length + 1;
        objects[path] = {
          'org.bluez.GattCharacteristic1': {
            'Service': DBusObjectPath(servicePath),
            'UUID': DBusString(services[s][c]),
            'Flags': DBusArray.string(['read']),
          },
        };
      }
    }
  }

  @override
  Future<DBusMethodSuccessResponse> callMethod({
    String? destination,
    required DBusObjectPath path,
    String? interface,
    required String name,
    Iterable<DBusValue> values = const [],
    DBusSignature? replySignature,
    bool noReplyExpected = false,
    bool noAutoStart = false,
    bool allowInteractiveAuthorization = false,
  }) async {
    if (destination == 'org.freedesktop.DBus') {
      if (name == 'GetNameOwner') {
        return DBusMethodSuccessResponse([const DBusString(':1.1')]);
      }
      if (name == 'AddMatch' || name == 'RemoveMatch') {
        return DBusMethodSuccessResponse();
      }
    }
    if (destination == 'org.bluez' &&
        path == DBusObjectPath.root &&
        interface == 'org.freedesktop.DBus.ObjectManager' &&
        name == 'GetManagedObjects') {
      return DBusMethodSuccessResponse([
        DBusDict(
          DBusSignature('o'),
          DBusSignature('a{sa{sv}}'),
          objects.map((path, interfaces) => MapEntry(
                path,
                DBusDict(
                  DBusSignature('s'),
                  DBusSignature('a{sv}'),
                  interfaces.map((name, properties) => MapEntry(DBusString(name), DBusDict.stringVariant(properties))),
                ),
              )),
        ),
      ]);
    }
    if (destination == 'org.bluez' &&
        interface == 'org.bluez.GattCharacteristic1' &&
        name == 'ReadValue' &&
        this.values.containsKey(path)) {
      reads.add(path);
      return DBusMethodSuccessResponse([
        DBusArray.byte([this.values[path]!]),
      ]);
    }
    throw StateError('Unexpected D-Bus call: $destination $path $interface.$name');
  }
}

/// Creates real BlueZ objects with deterministic, synthetic GATT data.
Future<(FlutterBluePlusLinux, BlueZClient, _GattBus)> _plugin(List<List<String>> services) async {
  final bus = _GattBus(services);
  final client = BlueZClient(bus: bus);
  addTearDown(() async {
    await client.close();
    await bus.close();
  });
  await client.connect().timeout(_deadline);
  return (FlutterBluePlusLinux(bluezClient: client), client, bus);
}

Future<BmDiscoverServicesResult> _discover(FlutterBluePlusLinux plugin) async {
  final result = plugin.onDiscoveredServices.first.timeout(_deadline);
  expect(
    await plugin.discoverServices(BmDiscoverServicesRequest(remoteId: DeviceIdentifier(_address))).timeout(_deadline),
    isTrue,
  );
  final event = await result;
  expect(event.success, isTrue);
  return event;
}

/// Checks both the selected D-Bus object path and the plugin's result identity.
Future<void> _read(FlutterBluePlusLinux plugin, _GattBus bus, String uuid, int instanceId, String path) async {
  final result = plugin.onCharacteristicReceived.first.timeout(_deadline);
  final success = await plugin
      .readCharacteristic(BmReadCharacteristicRequest(
        remoteId: DeviceIdentifier(_address),
        serviceUuid: Guid(_serviceUuid),
        characteristicUuid: Guid(uuid),
        instanceId: instanceId,
      ))
      .timeout(_deadline);
  final event = await result;
  expect(success, isTrue);
  expect(event.success, isTrue);
  expect(event.characteristicUuid, Guid(uuid));
  expect(event.instanceId, instanceId);
  expect(bus.reads.last, DBusObjectPath(path));
  expect(event.value, [bus.values[DBusObjectPath(path)]!]);
}

Future<void> _reject(FlutterBluePlusLinux plugin, _GattBus bus, String uuid, int instanceId) async {
  final previousReads = bus.reads.length;
  final result = plugin.onCharacteristicReceived.first.timeout(_deadline);
  final success = await plugin
      .readCharacteristic(BmReadCharacteristicRequest(
        remoteId: DeviceIdentifier(_address),
        serviceUuid: Guid(_serviceUuid),
        characteristicUuid: Guid(uuid),
        instanceId: instanceId,
      ))
      .timeout(_deadline);
  final event = await result;
  expect(success, isFalse);
  expect(event.success, isFalse);
  expect(event.instanceId, instanceId);
  expect(bus.reads, hasLength(previousReads));
}

void main() {
  test('BlueZ recreates wrappers over the same cached characteristic', () async {
    final (_, client, _) = await _plugin([
      [_levelUuid, _levelUuid],
    ]);
    final firstService = client.devices.single.gattServices.single;
    final nextService = client.devices.single.gattServices.single;
    expect(identical(firstService, nextService), isFalse);
    final first = firstService.characteristics.first;
    final next = nextService.characteristics.first;
    expect(identical(first, next), isFalse);
    expect(first.propertiesChanged, next.propertiesChanged);
    expect(first.propertiesChanged, isNot(nextService.characteristics.last.propertiesChanged));
  });

  test('single service discovery uses per-UUID ordinals', () async {
    final (plugin, _, _) = await _plugin([
      [_levelUuid, _statusUuid, _levelUuid, _levelUuid],
    ]);
    for (var i = 0; i < 2; i++) {
      final event = await _discover(plugin);
      expect(event.services.single.characteristics.map((c) => c.instanceId), [0, 0, 1, 2]);
    }
  });

  test('single service selects each repeated UUID instance', () async {
    final (plugin, _, bus) = await _plugin([
      [_levelUuid, _statusUuid, _levelUuid, _levelUuid],
    ]);
    await _discover(plugin);
    for (var i = 0; i < 3; i++) {
      final characteristic = [0, 2, 3][i];
      await _read(plugin, bus, _levelUuid, i, '$_devicePath/service0/char$characteristic');
    }
  });

  test('distinct UUIDs in one service each start at zero', () async {
    final (plugin, _, bus) = await _plugin([
      [_levelUuid, _statusUuid],
    ]);
    final event = await _discover(plugin);
    expect(event.services.single.characteristics.map((c) => c.instanceId), [0, 0]);
    await _read(plugin, bus, _levelUuid, 0, '$_devicePath/service0/char0');
    await _read(plugin, bus, _statusUuid, 0, '$_devicePath/service0/char1');
  });

  test('duplicate service UUIDs use flattened characteristic ordinals', () async {
    final (plugin, _, _) = await _plugin([
      [_levelUuid, _levelUuid, _statusUuid],
      [_levelUuid, _statusUuid],
    ]);
    for (var i = 0; i < 2; i++) {
      final event = await _discover(plugin);
      expect(event.services.map((s) => s.characteristics.map((c) => c.instanceId).toList()), [
        [0, 1, 2],
        [3, 4],
      ]);
    }
  });

  test('duplicate service UUIDs select by flattened ordinal and UUID', () async {
    final (plugin, _, bus) = await _plugin([
      [_levelUuid, _levelUuid, _statusUuid],
      [_levelUuid, _statusUuid],
    ]);
    await _discover(plugin);
    await _read(plugin, bus, _levelUuid, 0, '$_devicePath/service0/char0');
    await _read(plugin, bus, _levelUuid, 1, '$_devicePath/service0/char1');
    await _read(plugin, bus, _statusUuid, 2, '$_devicePath/service0/char2');
    await _read(plugin, bus, _levelUuid, 3, '$_devicePath/service1/char0');
    await _read(plugin, bus, _statusUuid, 4, '$_devicePath/service1/char1');
  });

  test('single service rejects invalid instances without a D-Bus read', () async {
    final (plugin, _, bus) = await _plugin([
      [_levelUuid, _levelUuid, _statusUuid],
    ]);
    await _discover(plugin);
    await _reject(plugin, bus, _levelUuid, -1);
    await _reject(plugin, bus, _levelUuid, 2);
    await _reject(plugin, bus, _statusUuid, 1);
  });

  test('duplicate services reject invalid instances and mismatched UUIDs', () async {
    final (plugin, _, bus) = await _plugin([
      [_levelUuid, _statusUuid],
      [_levelUuid],
    ]);
    await _discover(plugin);
    await _reject(plugin, bus, _levelUuid, -1);
    await _reject(plugin, bus, _levelUuid, 3);
    await _reject(plugin, bus, _levelUuid, 1);
    await _reject(plugin, bus, _statusUuid, 2);
  });
}
