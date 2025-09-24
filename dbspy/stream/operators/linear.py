from typing import Optional, TypeVar

from dbspy.stream import (
    Lift1,
    LiftedGroupAdd,
    LiftedGroupNegate,
    Stream,
    StreamAddition,
    StreamHandle,
    UnaryOperator,
    step_until_fixpoint,
    step_until_fixpoint_and_return,
)

T = TypeVar("T")


class Delay(UnaryOperator[T, T]):
    """Delays the input stream by one timestamp"""

    def __init__(self, stream: Optional[StreamHandle[T]]) -> None:
        super().__init__(stream, None)
    
    def step(self) -> bool:
        """Outputs the previous value from the input stream"""
        output_timestamp = self.output().get_current_time()
        input_timestamp = self.input_a().get_current_time()
    
        if output_timestamp <= input_timestamp:
            self.output().send(self.input_a()[output_timestamp])
            return False
    
        return True


class Differentiate(UnaryOperator[T, T]):
    """Computes the difference between consecutive elements in the input stream"""

    def __init__(self, stream: StreamHandle[T]) -> None:
        self.input_stream_handle = stream
        self.delayed_stream = Delay(self.input_stream_handle)
        self.delayed_negated_stream = LiftedGroupNegate(self.delayed_stream.output_handle())
        self.differentiation_stream = LiftedGroupAdd(
            self.input_stream_handle, self.delayed_negated_stream.output_handle()
        )
        self.output_stream_handle = self.differentiation_stream.output_handle()
    
    def step(self) -> bool:
        """Outputs the difference between the latest input stream element with the one before it"""
        self.delayed_stream.step()
        self.delayed_negated_stream.step()
        self.differentiation_stream.step()
        return self.output().get_current_time() == self.input_a().get_current_time()


class Integrate(UnaryOperator[T, T]):
    """Computes the running sum of the input stream"""
    
    def __init__(self, stream: StreamHandle[T]) -> None:
        self.input_stream_handle = stream
        self.integration_stream = LiftedGroupAdd(self.input_stream_handle, None)
        self.delayed_stream = Delay(self.integration_stream.output_handle())
        self.integration_stream.set_input_b(self.delayed_stream.output_handle())
        self.output_stream_handle = self.integration_stream.output_handle()
    
    def step(self) -> bool:
        """Adds the latest element from input stream to the running sum"""
        self.delayed_stream.step()
        self.integration_stream.step()
        return self.output().get_current_time() == self.input_a().get_current_time()
    

def step_until_fixpoint_set_new_default_then_return[T](
    operator: Integrate[T] | Delay[T],
) -> Stream[T]:
    step_until_fixpoint(operator)

    out = operator.output_handle().get()
    latest = out.latest()
    out.set_default(latest)

    return out


class LiftedDelay(Lift1[Stream[T], Stream[T]]):
    """Lifts the delay operator to work on streams of streams"""
    def __init__(self, stream: StreamHandle[Stream[T]]):
        super().__init__(
            stream,
            lambda s: step_until_fixpoint_set_new_default_then_return(
                Delay(StreamHandle(lambda: s))
            ),
            None
        )


class LiftedIntegrate(Lift1[Stream[T], Stream[T]]):
    """Lifts the Integrate operator to work on streams of streams"""
    def __init__(self, stream: StreamHandle[Stream[T]]):
        super().__init__(
            stream,
            lambda s: step_until_fixpoint_set_new_default_then_return(
                Integrate(StreamHandle(lambda: s))
            ),
            None
        )


class LiftedDifferentiate(Lift1[Stream[T], Stream[T]]):
    """Lifts the Differentiate operator to work on streams of streams"""
    def __init__(self, stream: StreamHandle[Stream[T]]):
        super().__init__(
            stream,
            lambda s: step_until_fixpoint_and_return(
                Differentiate(StreamHandle(lambda: s))
            ),
            None
        )
