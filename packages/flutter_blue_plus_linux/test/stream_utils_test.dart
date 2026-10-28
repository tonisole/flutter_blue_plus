import 'dart:async';

import 'package:flutter_blue_plus_linux/src/stream_utils.dart';
import 'package:flutter_test/flutter_test.dart';

// Drain the finite microtask chains used by stream delivery and cancellation.
// Cancellation gates below control ordering without relying on wall-clock delays.
Future<void> flushMicrotasks() async {
  for (var i = 0; i < 20; i++) {
    await Future<void>.value();
  }
}

void main() {
  group('switchMap', () {
    late StreamController<int> outer;
    late List<StreamController<int>> inners;
    late List<Completer<void>> gates;
    StreamSubscription<int>? subscription;

    StreamController<int> inner({void Function()? onListen, FutureOr<void> Function()? onCancel}) {
      final controller = StreamController<int>(sync: true, onListen: onListen, onCancel: onCancel);
      inners.add(controller);
      return controller;
    }

    Completer<void> gate() {
      final completer = Completer<void>();
      gates.add(completer);
      return completer;
    }

    setUp(() {
      outer = StreamController<int>(sync: true);
      inners = [];
      gates = [];
      subscription = null;
    });

    tearDown(() async {
      for (final completer in gates) {
        if (!completer.isCompleted) completer.complete();
      }
      try {
        await subscription?.cancel();
      } catch (_) {
        // Tests explicitly assert cancellation failures before teardown.
      }
      unawaited(outer.close());
      for (final controller in inners) {
        unawaited(controller.close());
      }
    });

    test('cancellation before the switch resumes never invokes the mapper', () async {
      final mapped = <int>[];
      var listens = 0;
      final source = inner(onListen: () => listens++);
      subscription = outer.stream.switchMap((event) {
        mapped.add(event);
        return source.stream;
      }).listen((_) {});

      outer.add(1);
      await subscription!.cancel();
      await flushMicrotasks();

      expect(mapped, isEmpty);
      expect(listens, 0);
    });

    test('only the newest rapid event installs an inner subscription', () async {
      final mapped = <int>[];
      final values = <int>[];
      final sources = [inner(), inner(), inner()];
      subscription = outer.stream.switchMap((event) {
        mapped.add(event);
        return sources[event - 1].stream;
      }).listen(values.add);

      outer.add(1);
      outer.add(2);
      outer.add(3);
      await flushMicrotasks();

      expect(mapped, [3]);
      expect(sources.map((source) => source.hasListener), [false, false, true]);
      sources.last.add(30);
      await flushMicrotasks();
      expect(values, [30]);
    });

    test('downstream cancellation waits for held inner cancellation and prevents replacement', () async {
      final release = gate();
      var cancellations = 0;
      final first = inner(onCancel: () {
        cancellations++;
        return release.future;
      });
      final next = inner();
      final mapped = <int>[];
      subscription = outer.stream.switchMap((event) {
        mapped.add(event);
        return event == 1 ? first.stream : next.stream;
      }).listen((_) {});

      outer.add(1);
      await flushMicrotasks();
      outer.add(2);
      await flushMicrotasks();
      var canceled = false;
      final cancellation = subscription!.cancel().then((_) => canceled = true);
      await flushMicrotasks();
      expect(canceled, isFalse);
      expect(mapped, [1]);

      release.complete();
      await cancellation;
      await flushMicrotasks();
      expect(mapped, [1]);
      expect(cancellations, 1);
      expect(next.hasListener, isFalse);
    });

    test('superseding events share held cancellation and only the newest is mapped', () async {
      final release = gate();
      var cancellations = 0;
      final first = inner(onCancel: () {
        cancellations++;
        return release.future;
      });
      final middle = inner();
      final last = inner();
      final mapped = <int>[];
      final values = <int>[];
      subscription = outer.stream.switchMap((event) {
        mapped.add(event);
        return [first, middle, last][event - 1].stream;
      }).listen(values.add);

      outer.add(1);
      await flushMicrotasks();
      outer.add(2);
      await flushMicrotasks();
      outer.add(3);
      await flushMicrotasks();
      expect(mapped, [1]);

      release.complete();
      await flushMicrotasks();
      expect(mapped, [1, 3]);
      expect(cancellations, 1);
      expect(middle.hasListener, isFalse);
      expect(last.hasListener, isTrue);
      last.add(30);
      await flushMicrotasks();
      expect(values, [30]);
    });

    test('mapper errors reach the listener with their stack and allow recovery', () async {
      final failure = StateError('mapper failed');
      final stack = StackTrace.current;
      final errors = <Object>[];
      final stacks = <StackTrace>[];
      final values = <int>[];
      final source = inner();
      subscription = outer.stream.switchMap((event) {
        if (event == 1) Error.throwWithStackTrace(failure, stack);
        return source.stream;
      }).listen(values.add, onError: (Object error, StackTrace trace) {
        errors.add(error);
        stacks.add(trace);
      });

      outer.add(1);
      await flushMicrotasks();
      expect(errors, [same(failure)]);
      expect(stacks.single.toString(), stack.toString());
      outer.add(2);
      await flushMicrotasks();
      source.add(20);
      await flushMicrotasks();
      expect(values, [20]);
    });

    test('replacement cancellation errors reach the listener and allow recovery', () async {
      final failure = StateError('cancel failed');
      final stack = StackTrace.current;
      final first = inner(onCancel: () => Future<void>.error(failure, stack));
      final next = inner();
      final mapped = <int>[];
      final errors = <Object>[];
      final stacks = <StackTrace>[];
      subscription = outer.stream.switchMap((event) {
        mapped.add(event);
        return event == 1 ? first.stream : next.stream;
      }).listen((_) {}, onError: (Object error, StackTrace trace) {
        errors.add(error);
        stacks.add(trace);
      });

      outer.add(1);
      await flushMicrotasks();
      outer.add(2);
      await flushMicrotasks();
      expect(errors, [same(failure)]);
      expect(stacks.single.toString(), stack.toString());
      expect(mapped, [1]);
      outer.add(3);
      await flushMicrotasks();
      expect(mapped, [1, 3]);
      expect(next.hasListener, isTrue);
    });

    test('a superseded cancellation failure never escapes to the root zone', () async {
      final release = gate();
      final failure = StateError('cancel failed');
      final first = inner(onCancel: () => release.future);
      final mapped = <int>[];
      final errors = <Object>[];
      subscription = outer.stream.switchMap((event) {
        mapped.add(event);
        return event == 1 ? first.stream : inner().stream;
      }).listen((_) {}, onError: errors.add);

      outer.add(1);
      await flushMicrotasks();
      outer.add(2);
      outer.add(3);
      await flushMicrotasks();
      release.completeError(failure);
      await flushMicrotasks();

      expect(errors, [same(failure)]);
      expect(mapped, [1]);
    });

    test('downstream cancellation cleans up the outer stream even if inner cancellation fails', () async {
      final failure = StateError('cancel failed');
      final source = inner(onCancel: () => Future<void>.error(failure));
      var outerCanceled = false;
      outer.onCancel = () => outerCanceled = true;
      subscription = outer.stream.switchMap((_) => source.stream).listen((_) {});
      outer.add(1);
      await flushMicrotasks();

      await expectLater(subscription!.cancel(), throwsA(same(failure)));
      expect(outerCanceled, isTrue);
    });

    test('cancellation during a failing replacement joins cleanup without an uncaught error', () async {
      final release = gate();
      final failure = StateError('cancel failed');
      final source = inner(onCancel: () => release.future);
      final errors = <Object>[];
      subscription = outer.stream.switchMap((_) => source.stream).listen((_) {}, onError: errors.add);
      outer.add(1);
      await flushMicrotasks();
      outer.add(2);
      await flushMicrotasks();
      final cancellation = expectLater(subscription!.cancel(), throwsA(same(failure)));
      release.completeError(failure);
      await cancellation;
      await flushMicrotasks();
      expect(errors, isEmpty);
    });

    test('outer completion waits for a pending first switch and its inner data', () async {
      final values = <int>[];
      var done = false;
      subscription = outer.stream.switchMap((event) => Stream.value(event * 10)).listen(
            values.add,
            onDone: () => done = true,
          );
      outer.add(1);
      unawaited(outer.close());
      await flushMicrotasks();

      expect(values, [10]);
      expect(done, isTrue);
    });

    test('outer completion waits for held replacement and the final inner stream', () async {
      final release = gate();
      final first = inner(onCancel: () => release.future);
      final last = inner();
      final values = <int>[];
      var done = false;
      subscription = outer.stream.switchMap((event) => event == 1 ? first.stream : last.stream).listen(
            values.add,
            onDone: () => done = true,
          );
      outer.add(1);
      await flushMicrotasks();
      outer.add(2);
      outer.add(3);
      unawaited(outer.close());
      await flushMicrotasks();
      expect(done, isFalse);
      release.complete();
      await flushMicrotasks();
      last.add(30);
      await last.close();
      await flushMicrotasks();

      expect(values, [30]);
      expect(done, isTrue);
    });

    test('a mapper error after outer completion is delivered before done', () async {
      final failure = StateError('mapper failed');
      final events = <Object>[];
      subscription = outer.stream.switchMap<int>((_) => throw failure).listen(
            events.add,
            onError: events.add,
            onDone: () => events.add('done'),
          );
      outer.add(1);
      unawaited(outer.close());
      await flushMicrotasks();

      expect(events, [same(failure), 'done']);
    });

    test('a cancellation error after outer completion is delivered before done', () async {
      final failure = StateError('cancel failed');
      final first = inner(onCancel: () => Future<void>.error(failure));
      final events = <Object>[];
      subscription = outer.stream.switchMap((_) => first.stream).listen(
            events.add,
            onError: events.add,
            onDone: () => events.add('done'),
          );
      outer.add(1);
      await flushMicrotasks();
      outer.add(2);
      unawaited(outer.close());
      await flushMicrotasks();

      expect(events, [same(failure), 'done']);
    });

    test('empty outer stream completes without invoking the mapper', () async {
      var mapped = false;
      var done = false;
      subscription = outer.stream.switchMap((_) {
        mapped = true;
        return inner().stream;
      }).listen((_) {}, onDone: () => done = true);
      unawaited(outer.close());
      await flushMicrotasks();
      expect(mapped, isFalse);
      expect(done, isTrue);
    });

    test('quickly completing empty inner streams do not prevent completion', () async {
      var done = false;
      subscription = outer.stream.switchMap((_) => const Stream<int>.empty()).listen(
            (_) {},
            onDone: () => done = true,
          );
      outer.add(1);
      await flushMicrotasks();
      outer.add(2);
      unawaited(outer.close());
      await flushMicrotasks();
      expect(done, isTrue);
    });

    test('inner completion before outer completion permits a later switch', () async {
      final values = <int>[];
      var done = false;
      subscription = outer.stream.switchMap((event) => Stream.value(event)).listen(
            values.add,
            onDone: () => done = true,
          );
      outer.add(1);
      await flushMicrotasks();
      expect(values, [1]);
      expect(done, isFalse);
      outer.add(2);
      await flushMicrotasks();
      unawaited(outer.close());
      await flushMicrotasks();
      expect(values, [1, 2]);
      expect(done, isTrue);
    });

    test('normal data and source errors are forwarded until the last inner completes', () async {
      final first = inner();
      final last = inner();
      final values = <int>[];
      final errors = <Object>[];
      final outerError = StateError('outer error');
      final innerError = StateError('inner error');
      var done = false;
      subscription = outer.stream.switchMap((event) => event == 1 ? first.stream : last.stream).listen(
            values.add,
            onError: errors.add,
            onDone: () => done = true,
          );
      outer.add(1);
      await flushMicrotasks();
      first.add(10);
      outer.addError(outerError);
      outer.add(2);
      await flushMicrotasks();
      last.add(20);
      last.addError(innerError);
      unawaited(outer.close());
      await flushMicrotasks();
      expect(done, isFalse);
      await last.close();
      await flushMicrotasks();
      expect(values, [10, 20]);
      expect(errors, [same(outerError), same(innerError)]);
      expect(done, isTrue);
    });

    test('onListen replacements join late cleanup and only the newest is mapped', () async {
      final release = gate();
      var cancellations = 0;
      final first = inner(
        onListen: () {
          outer.add(2);
          outer.add(3);
        },
        onCancel: () {
          cancellations++;
          return release.future;
        },
      );
      final middle = inner();
      final last = inner();
      final mapped = <int>[];
      final values = <int>[];
      var done = false;
      subscription = outer.stream.switchMap((event) {
        mapped.add(event);
        return [first, middle, last][event - 1].stream;
      }).listen(values.add, onDone: () => done = true);

      outer.add(1);
      await flushMicrotasks();
      unawaited(outer.close());
      await flushMicrotasks();
      expect(mapped, [1]);
      expect(cancellations, 1);
      expect(middle.hasListener, isFalse);
      expect(last.hasListener, isFalse);
      expect(done, isFalse);

      release.complete();
      await flushMicrotasks();
      expect(mapped, [1, 3]);
      expect(middle.hasListener, isFalse);
      expect(last.hasListener, isTrue);
      expect(done, isFalse);
      last.add(30);
      await last.close();
      await flushMicrotasks();
      expect(values, [30]);
      expect(cancellations, 1);
      expect(done, isTrue);
    });

    test('onListen downstream cancellation waits for late cleanup', () async {
      final release = gate();
      Future<void>? cancellation;
      var canceled = false;
      var outerCanceled = false;
      var cancellations = 0;
      outer.onCancel = () => outerCanceled = true;
      final source = inner(
        onListen: () {
          cancellation = subscription!.cancel().then((_) => canceled = true);
        },
        onCancel: () {
          cancellations++;
          return release.future;
        },
      );
      subscription = outer.stream.switchMap((_) => source.stream).listen((_) {});

      outer.add(1);
      await flushMicrotasks();
      expect(cancellation, isNotNull);
      expect(cancellations, 1);
      expect(canceled, isFalse);

      release.complete();
      await cancellation;
      expect(canceled, isTrue);
      expect(outerCanceled, isTrue);
      expect(cancellations, 1);
      expect(source.hasListener, isFalse);
    });

    test('onListen late cleanup errors reach the newest replacement and allow recovery', () async {
      final release = gate();
      final failure = StateError('late cleanup failed');
      final stack = StackTrace.current;
      var cancellations = 0;
      final first = inner(
        onListen: () {
          outer.add(2);
          outer.add(3);
        },
        onCancel: () {
          cancellations++;
          return release.future;
        },
      );
      final next = inner();
      final mapped = <int>[];
      final values = <int>[];
      final errors = <Object>[];
      final stacks = <StackTrace>[];
      var done = false;
      subscription = outer.stream.switchMap((event) {
        mapped.add(event);
        return event == 1 ? first.stream : next.stream;
      }).listen(
        values.add,
        onError: (Object error, StackTrace trace) {
          errors.add(error);
          stacks.add(trace);
        },
        onDone: () => done = true,
      );

      outer.add(1);
      await flushMicrotasks();
      release.completeError(failure, stack);
      await flushMicrotasks();
      expect(errors, [same(failure)]);
      expect(stacks.single.toString(), stack.toString());
      expect(mapped, [1]);
      expect(next.hasListener, isFalse);
      expect(cancellations, 1);

      outer.add(4);
      await flushMicrotasks();
      expect(mapped, [1, 4]);
      next.add(40);
      unawaited(outer.close());
      await next.close();
      await flushMicrotasks();
      expect(values, [40]);
      expect(errors, [same(failure)]);
      expect(done, isTrue);
    });

    test('onListen late cleanup errors reach downstream cancellation with their stack', () async {
      final release = gate();
      final failure = StateError('late cleanup failed');
      final stack = StackTrace.current;
      final cancelErrors = <Object>[];
      final cancelStacks = <StackTrace>[];
      final streamErrors = <Object>[];
      Future<void>? cancellation;
      var canceledSuccessfully = false;
      var outerCanceled = false;
      var cancellations = 0;
      outer.onCancel = () => outerCanceled = true;
      final source = inner(
        onListen: () {
          cancellation = subscription!.cancel().then((_) {
            canceledSuccessfully = true;
          }, onError: (Object error, StackTrace trace) {
            cancelErrors.add(error);
            cancelStacks.add(trace);
          });
        },
        onCancel: () {
          cancellations++;
          return release.future;
        },
      );
      subscription = outer.stream.switchMap((_) => source.stream).listen((_) {}, onError: streamErrors.add);

      outer.add(1);
      await flushMicrotasks();
      expect(cancellation, isNotNull);
      release.completeError(failure, stack);
      await cancellation;
      await flushMicrotasks();
      expect(cancelErrors, [same(failure)]);
      expect(cancelStacks.single.toString(), stack.toString());
      expect(canceledSuccessfully, isFalse);
      expect(streamErrors, isEmpty);
      expect(outerCanceled, isTrue);
      expect(cancellations, 1);
    });

    test('downstream cancellation joins held cleanup from an onListen replacement', () async {
      final release = gate();
      var cancellations = 0;
      final first = inner(
        onListen: () => outer.add(2),
        onCancel: () {
          cancellations++;
          return release.future;
        },
      );
      final next = inner();
      final mapped = <int>[];
      subscription = outer.stream.switchMap((event) {
        mapped.add(event);
        return event == 1 ? first.stream : next.stream;
      }).listen((_) {});

      outer.add(1);
      await flushMicrotasks();
      var canceled = false;
      final cancellation = subscription!.cancel().then((_) => canceled = true);
      await flushMicrotasks();
      expect(canceled, isFalse);
      expect(mapped, [1]);
      expect(next.hasListener, isFalse);

      release.complete();
      await cancellation;
      await flushMicrotasks();
      expect(mapped, [1]);
      expect(cancellations, 1);
      expect(next.hasListener, isFalse);
    });

    for (final cleanup in ['success', 'failure', 'throw']) {
      test('ordinary onCancel reentrant cancellation joins cleanup ($cleanup)', () async {
        final release = gate();
        final failure = StateError('cleanup failed');
        final stack = StackTrace.current;
        final cancelErrors = <Object>[];
        final cancelStacks = <StackTrace>[];
        final streamErrors = <Object>[];
        final mapped = <int>[];
        Future<void>? cancellation;
        var completed = false;
        var canceledSuccessfully = false;
        var outerCanceled = false;
        var cancellations = 0;
        outer.onCancel = () => outerCanceled = true;
        final first = inner(onCancel: () {
          cancellations++;
          cancellation = subscription!.cancel().then((_) {
            completed = true;
            canceledSuccessfully = true;
          }, onError: (Object error, StackTrace trace) {
            completed = true;
            cancelErrors.add(error);
            cancelStacks.add(trace);
          });
          if (cleanup == 'throw') Error.throwWithStackTrace(failure, stack);
          return release.future;
        });
        final next = inner();
        subscription = outer.stream.switchMap((event) {
          mapped.add(event);
          return event == 1 ? first.stream : next.stream;
        }).listen((_) {}, onError: streamErrors.add);

        outer.add(1);
        await flushMicrotasks();
        expect(first.hasListener, isTrue);
        outer.add(2);
        await flushMicrotasks();
        final completedBeforeRelease = completed;
        final outerCanceledBeforeRelease = outerCanceled;
        if (cleanup == 'failure') {
          release.completeError(failure, stack);
        } else {
          release.complete();
        }
        await cancellation;
        await flushMicrotasks();

        expect(cancellation, isNotNull);
        expect(completed, isTrue);
        expect(cancellations, 1);
        expect(mapped, [1]);
        expect(next.hasListener, isFalse);
        expect(first.hasListener, isFalse);
        expect(outerCanceled, isTrue);
        expect(streamErrors, isEmpty);
        if (cleanup == 'success') {
          expect(cancelErrors, isEmpty);
          expect(canceledSuccessfully, isTrue);
        } else {
          expect(cancelErrors, [same(failure)]);
          expect(cancelStacks.single.toString(), stack.toString());
          expect(canceledSuccessfully, isFalse);
        }
        if (cleanup != 'throw') {
          expect(completedBeforeRelease, isFalse);
          expect(outerCanceledBeforeRelease, isFalse);
        }
      });
    }
  });
}
