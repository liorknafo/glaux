-- error: Cannot negate the operand of unary '-'
-- DataFusion refuses this too, but with its own planner text (and without
-- naming the operand type, which is why the message does not either).
SELECT -'a'
