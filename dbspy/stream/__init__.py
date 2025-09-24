import sys
from abc import abstractmethod
from types import NotImplementedType
from typing import Callable, Iterator, List, Optional
from typing import OrderedDict, Protocol, TypeVar, cast

from dbspy.core import AbelianGroupOperation


class Stream[T]:
    """
        Represents a stream of elements that are from an Abelian Group
    """

    timestamp: int
    inner: OrderedDict[int, T]
    group_op: AbelianGroupOperation[T]
    identity: bool
    default: T
    default_changes: OrderedDict[int, T]

    def __init__(self, group_op: AbelianGroupOperation[T]) -> None:
        self.inner = OrderedDict()
        self.group_op = group_op
        self.timestamp = -1
        # indicates if this whole stream is 'identity' stream?
        self.identity = True
        self.default = group_op.identity()
        self.default_changes = OrderedDict()
        self.default_changes[0] = group_op.identity()
        self.send(group_op.identity())

    def __repr__(self) -> str:
        return self.inner.__repr__()

    def send(self, element: T) -> None:
        """Adds an element to the stream and increments the timestamp"""
        if element != self.default:
            self.inner[self.timestamp + 1] = element
            self.identity = False
        
        self.timestamp += 1
    
    def group(self) -> AbelianGroupOperation[T]:
        """Returns the Abelian Group Operation associated with this stream"""
        return self.group_op
    
    def get_current_time(self) -> int:
        """Returns the current timestamp of this stream"""
        return self.timestamp

    def set_default(self, new_default: T) -> None:
        """LEAVING EMPTY FOR NOW.. NO IDEA WHAT THIS FUNCTION IS SUPPOSED TO DO"""
        return NotImplementedError

    def __getitem__(self, timestamp: int) -> T:
        """Returns an element at a given timestamp"""
        if timestamp < 0:
            raise ValueError("Timestamp cannot be negative")
        
        if timestamp <= self.get_current_time():
            default_timestamp = max((t for t in self.default_changes if t < timestamp), default=0)
            return self.inner.get(timestamp, self.default_changes[default_timestamp])

        elif timestamp > self.get_current_time():
            while timestamp > self.get_current_time():
                self.send(self.default)
        
            return self.__getitem__(timestamp)

    def __iter__(self) -> Iterator[T]:
        """Allows doing loops on this stream"""
        for t in range(self.get_current_time() + 1):
            yield self[t]
    
    def latest(self) -> T:
        """Returns the most recent element"""
        return self.__getitem__(self.get_current_time())
    
    def is_identity_stream(self) -> bool:
        """Is the stream still an identity stream?"""
        return self.identity

    def to_list(self) -> List[T]:
        """Convert stream (the timestamped dictionary) to a list.. can be done because __iter__ is defined"""
        return list(iter(self))
    
    def __eq__(self, other: object) -> bool:
        """Compaares this stream with another stream, considering all timestamps up to the latest"""
        if not isinstance(other, Stream):
            return NotImplemented
    
        # both are identity? must be equal
        if self.is_identity_stream() and other.is_identity_stream():
            return True

        # timestamps don't match? can't be equal
        self_timestamp = self.get_current_time()
        other_timestamp = other.get_current_time()
        if self_timestamp != other_timestamp:
            return False
    
        # all elements must match
        return self.inner == other.inner

T = TypeVar("T")
R = TypeVar("R")
S = TypeVar("S")

# we have defined the stream reference as a callable function
StreamReference = Callable[[], Stream[T]]

class StreamHandle[T]:
    """A handle to a stream, allowing lazy access."""

    def __init__(self, stream_reference: StreamReference[T]) -> None:
        self.ref = stream_reference
    
    def get(self) -> Stream[T]:
        """Returns the referenced stream"""
        return self.ref()


class Operator(Protocol[T]):
    @abstractmethod
    def step(self) -> bool:
        raise NotImplementedError
    
    @abstractmethod
    def output_handle(self) -> StreamHandle[T]:
        raise NotImplementedError


def step_until_fixpoint[T](operator: Operator[T]) -> None:
    while not operator.step():
        pass

def step_until_fixpoint_and_return[T](operator: Operator[T]) -> Stream[T]:
    step_until_fixpoint(operator)
    return operator.output_handle().get()


class UnaryOperator(Operator[R], Protocol[T, R]):
    # base class for unary stream operators
    
    input_stream_handle: StreamHandle[T]
    output_stream_handle: StreamHandle[R]
    
    def __init__(
        self,
        stream_handle: Optional[StreamHandle[T]],
        output_stream_group: Optional[AbelianGroupOperation[R]]
    ):
        if stream_handle is not None:
            self.set_input(stream_handle, output_stream_group)
    
    def set_input(
        self,
        stream_handle: StreamHandle[T],
        output_stream_group: Optional[AbelianGroupOperation[R]]
    ):
        """Sets the input stream and initializes the output stream."""
        self.input_stream_handle = stream_handle

        if output_stream_group is not None:
            output = Stream(output_stream_group)
            self.output_stream_handle = StreamHandle(lambda: output)
        else:
            output = Stream(self.input_a().group())
            self.output_stream_handle = StreamHandle(lambda: output)
        
    def output(self) -> Stream[R]:
        return self.output_stream_handle.get()

    def input_a(self) -> Stream[T]:
        return self.input_stream_handle.get()

    def output_handle(self) -> StreamHandle[R]:
        return StreamHandle(lambda: self.output())


class BinaryOperator(Operator[S], Protocol[T, R, S]):
    """Base class for stream operators with two inputs and one output"""

    input_stream_handle_a: StreamHandle[T]
    input_stream_handle_b: StreamHandle[R]
    output_stream_handle: StreamHandle[S]

    def __init__(
        self,
        stream_a: Optional[StreamHandle[T]],
        stream_b: Optional[StreamHandle[R]],
        output_stream_group: Optional[AbelianGroupOperation[S]]
    ) -> None:

        if stream_a is not None:
            self.set_input_a(stream_a)
        
        if stream_b is not None:
            self.set_input_b(stream_b)
        
        if output_stream_group is not None:
            output = Stream(output_stream_group)
            self.set_output_stream(StreamHandle(lambda: output))

    def set_input_a(self, stream_handle_a: StreamHandle[T]) -> None:
        """Sets the first input stream and inits the output stream"""
        self.input_stream_handle_a = stream_handle_a
        output = cast(Stream[S], Stream(self.input_a().group()))
        self.set_output_stream(StreamHandle(lambda: output))
    
    def set_input_b(self, stream_handle_b: StreamHandle[R]) -> None:
        """Sets the second input stream"""
        self.input_stream_handle_b = stream_handle_b
    
    def set_output_stream(self, output_stream_handle: StreamHandle[S]) -> None:
        """Sets the output stream handle"""
        self.output_stream_handle = output_stream_handle

    def output(self) -> Stream[S]:
        return self.output_stream_handle.get()

    def input_a(self) -> Stream[T]:
        return self.input_stream_handle_a.get()

    def input_b(self) -> Stream[R]:
        return self.input_stream_handle_b.get()
    
    def output_handle(self) -> StreamHandle[S]:
        handle = StreamHandle(lambda: self.output())
        return handle


# F1 is a type that takes a single input of type 'T' and returns a single 'R' type output
F1 = Callable[[T], R]

class Lift1(UnaryOperator[T, R]):
    """Lifts a unary function to operator on a stream"""

    f1: F1[T, R]
    def __init__(
        self, 
        stream: Optional[StreamHandle[T]],
        f1: F1[T, R],
        output_stream_group: Optional[AbelianGroupOperation[R]]
    ):
        self.f1 = f1
        self.frontier = 0
        super().__init__(stream, output_stream_group)

    def step(self) -> bool:
        """Applies the lifted function f1 to the next element in the input stream"""

        output_timestamp = self.output().get_current_time()
        input_timestamp = self.input_a().get_current_time()
        join = max(input_timestamp, output_timestamp, self.frontier)
        meet = min(input_timestamp, output_timestamp, self.frontier)
        if join == meet:
            return True
        
        next_frontier = self.frontier + 1
        self.output().send(self.f1(self.input_a()[next_frontier]))
        self.frontier = next_frontier

        return False


F2 = Callable[[T, R], S]

class Lift2(BinaryOperator[T, R, S]):
    """Lifts a binary function to operator on two streams"""

    f2: F2[T, R, S]
    frontier_a = int
    frontier_b = int

    def __init__(
        self,
        stream_a: Optional[StreamHandle[T]],
        stream_b: Optional[StreamHandle[R]],
        f2: F2[T, R, S],
        output_stream_group: Optional[AbelianGroupOperation[S]]
    ) -> None:

        self.f2 = f2
        self.frontier_a = 0
        self.frontier_b = 0

        super().__init__(stream_a, stream_b, output_stream_group)

    def step(self) -> bool:
        """Applies the lifted binary-input function to most recent elements in both input streams"""

        a_timestamp = self.input_a().get_current_time()
        b_timestamp = self.input_b().get_current_time()
        output_timestamp = self.output().get_current_time()

        join = max(a_timestamp, b_timestamp, output_timestamp, self.frontier_a, self.frontier_b)
        meet = min(a_timestamp, b_timestamp, output_timestamp, self.frontier_a, self.frontier_b)
        if join == meet:
            return True

        next_frontier_a = self.frontier_a + 1
        next_frontier_b = self.frontier_b + 1
        a = self.input_a()[next_frontier_a]
        b = self.input_b()[next_frontier_b]

        application = self.f2(a, b)
        self.output().send(application)

        self.frontier_a = next_frontier_a
        self.frontier_b = next_frontier_b

        return False


class LiftedGroupAdd(Lift2[T, T, T]):
    def __init__(self, stream_a: StreamHandle[T], stream_b: Optional[StreamHandle[T]]):
        super().__init__(
            stream_a,
            stream_b,
            lambda x, y: stream_a.get().group().add(x, y),
            None
        )


class LiftedGroupNegate(Lift1[T, T]):
    """Negates an incoming stream"""

    def __init__(self, stream: StreamHandle[T]):
        super().__init__(stream, lambda x: stream.get().group().neg(x), None)


class StreamAddition(AbelianGroupOperation[Stream[T]]):
    """Defines addition for streams by lifting their underlying group's addition"""

    group: AbelianGroupOperation[T]

    def __init__(self, group: AbelianGroupOperation[T]) -> None:
        self.group = group

    def add(self, a: Stream[T], b: Stream[T]) -> Stream[T]:
        """Adds two streams element-wise"""
        handle_a = StreamHandle(lambda: a)
        handle_b = StreamHandle(lambda: b)

        lifted_group_add = LiftedGroupAdd(handle_a, handle_b)
        out = step_until_fixpoint_and_return(lifted_group_add)
        if a.is_identity_stream():
            out.default = b.default
        if b.is_identity_stream():
            out.default = a.default
        
        return out
    
    def inner_group(self) -> AbelianGroupOperation[T]:
        """Returns the underlying group operation"""
        return self.group
    
    def neg(self, a: Stream[T]) -> Stream[T]:
        """Negates a stream element-size"""
        handle_a = StreamHandle(lambda: a)
        lifted_group_neg = LiftedGroupNegate(handle_a)
        return step_until_fixpoint_and_return(lifted_group_neg)
    
    def identity(self) -> Stream[T]:
        """Returns the identity stream for addition operation"""
        identity_stream = Stream(self.group)
        return identity_stream