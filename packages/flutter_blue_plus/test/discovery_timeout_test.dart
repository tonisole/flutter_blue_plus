import 'dart:async';

import 'package:flutter_blue_plus/flutter_blue_plus.dart';
import 'package:flutter_blue_plus_platform_interface/flutter_blue_plus_platform_interface.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test('public discovery forwards the default and caller-selected timeout', () async {
    final platform = _Platform();
    FlutterBluePlusPlatform.instance = platform;
    await FlutterBluePlus.isSupported;
    final device = BluetoothDevice.fromId('00:11:22:33:44:55');
    platform.connections.add(BmConnectionStateResponse(
      remoteId: device.remoteId,
      connectionState: BmConnectionStateEnum.connected,
      disconnectReasonCode: null,
      disconnectReasonString: null,
    ));
    await Future<void>.delayed(Duration.zero);
    expect(device.isConnected, isTrue);
    await device.discoverServices(subscribeToServicesChanged: false);
    await device.discoverServices(subscribeToServicesChanged: false, timeout: 37);
    expect(platform.timeouts, [15, 37]);
    await platform.connections.close();
    await platform.results.close();
  });
}

final class _Platform extends FlutterBluePlusPlatform {
  final connections = StreamController<BmConnectionStateResponse>.broadcast();
  final results = StreamController<BmDiscoverServicesResult>.broadcast();
  final timeouts = <int>[];

  @override
  Stream<BmConnectionStateResponse> get onConnectionStateChanged => connections.stream;
  @override
  Stream<BmDiscoverServicesResult> get onDiscoveredServices => results.stream;
  @override
  Future<bool> isSupported(BmIsSupportedRequest request) async => true;
  @override
  Future<bool> discoverServices(BmDiscoverServicesRequest request) async {
    timeouts.add(request.timeout);
    results.add(BmDiscoverServicesResult(
      remoteId: request.remoteId,
      services: [],
      success: true,
      errorCode: 0,
      errorString: '',
    ));
    return true;
  }
}
