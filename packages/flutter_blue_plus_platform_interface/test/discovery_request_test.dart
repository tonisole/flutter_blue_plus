import 'package:flutter_blue_plus_platform_interface/flutter_blue_plus_platform_interface.dart';
import 'package:flutter_test/flutter_test.dart';

void main() {
  test('discovery request defaults to 15 seconds for existing callers', () {
    final request = BmDiscoverServicesRequest(remoteId: DeviceIdentifier('00:11:22:33:44:55'));
    expect(request.timeout, 15);
  });

  test('discovery request preserves a caller-selected timeout', () {
    final request = BmDiscoverServicesRequest(remoteId: DeviceIdentifier('00:11:22:33:44:55'), timeout: 37);
    expect(request.timeout, 37);
  });
}
