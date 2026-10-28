import 'dart:async';

Stream<T> mergeStreams<T>(Iterable<Stream<T>> streams) {
  return Stream.multi(
    (controller) {
      final subscriptions = <StreamSubscription<T>>[];
      var remaining = 0;
      var isCanceled = false;

      Future<void> cancelAll() async {
        if (isCanceled) return;
        isCanceled = true;
        await Future.wait(
          subscriptions.map(
            (subscription) {
              return subscription.cancel();
            },
          ),
        );
      }

      void maybeClose() {
        if (!isCanceled && remaining == 0) {
          controller.close();
        }
      }

      for (final stream in streams) {
        remaining++;
        subscriptions.add(
          stream.listen(
            controller.add,
            onError: controller.addError,
            onDone: () {
              remaining--;
              maybeClose();
            },
          ),
        );
      }

      maybeClose();
      controller.onCancel = cancelAll;
    },
    isBroadcast: true,
  );
}

extension StreamUtils<T> on Stream<T> {
  Stream<T> mergeWith(Iterable<Stream<T>> streams) {
    return mergeStreams([this, ...streams]);
  }

  Stream<T> startWith(T initialValue) {
    return Stream.multi(
      (controller) {
        controller.add(initialValue);
        final subscription = listen(
          controller.add,
          onError: controller.addError,
          onDone: controller.close,
        );
        controller.onCancel = subscription.cancel;
      },
      isBroadcast: isBroadcast,
    );
  }

  Stream<S> switchMap<S>(Stream<S> Function(T event) mapper) {
    return Stream.multi(
      (controller) {
        StreamSubscription<T>? outerSubscription;
        StreamSubscription<S>? innerSubscription;
        Future<void>? innerCancellation;
        var outerDone = false;
        var isCanceled = false;
        var isSwitching = false;
        var generation = 0;

        void closeIfDone() {
          if (!isCanceled && outerDone && !isSwitching && innerSubscription == null) {
            controller.close();
          }
        }

        outerSubscription = listen(
          (event) async {
            final current = ++generation;
            isSwitching = true;
            try {
              // Detach once, but let every replacement await the same cleanup.
              final previous = innerSubscription;
              innerSubscription = null;
              if (previous != null) {
                // Publish before cancel can synchronously reenter onCancel.
                final pendingCancellation = Completer<void>();
                innerCancellation = pendingCancellation.future;
                pendingCancellation.complete(Future<void>.sync(previous.cancel));
              }
              final cancellation = innerCancellation;
              try {
                await cancellation;
              } finally {
                if (identical(innerCancellation, cancellation)) innerCancellation = null;
              }
              if (isCanceled || current != generation) return;

              final stream = mapper(event);
              if (isCanceled || current != generation) return;
              // Publish ownership before onListen can reenter, including any
              // cleanup needed if this listen is canceled or superseded.
              final pendingListen = Completer<void>();
              innerCancellation = pendingListen.future;
              try {
                var innerDone = false;
                final subscription = stream.listen(
                  controller.add,
                  onError: controller.addError,
                  onDone: () {
                    innerDone = true;
                    if (current != generation) return;
                    innerSubscription = null;
                    closeIfDone();
                  },
                );
                if (isCanceled || current != generation) {
                  pendingListen.complete(subscription.cancel());
                } else {
                  if (!innerDone) innerSubscription = subscription;
                  pendingListen.complete();
                }
              } catch (error, stack) {
                pendingListen.completeError(error, stack);
              }
              try {
                await pendingListen.future;
              } finally {
                if (identical(innerCancellation, pendingListen.future)) innerCancellation = null;
              }
            } catch (error, stack) {
              // Stream.listen does not await this callback's future.
              if (!isCanceled && current == generation) controller.addError(error, stack);
            } finally {
              if (current == generation) {
                isSwitching = false;
                closeIfDone();
              }
            }
          },
          onError: controller.addError,
          onDone: () {
            outerDone = true;
            closeIfDone();
          },
        );

        controller.onCancel = () async {
          isCanceled = true;
          generation++;
          final inner = innerSubscription;
          innerSubscription = null;
          try {
            await (inner?.cancel() ?? innerCancellation);
          } finally {
            await outerSubscription?.cancel();
          }
        };
      },
      isBroadcast: true,
    );
  }
}
