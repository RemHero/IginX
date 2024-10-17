package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.unary;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;

public class PhysicalCount extends UnaryAccumulator {

    public static final String NAME = "count";

    public PhysicalCount() {
        super(NAME);
    }

    @Override
    protected UnaryState initializeImpl(BufferAllocator allocator, Field field)
        throws ComputeException {
        return new CountState(allocator, field, this);
    }

    protected static class CountState extends UnaryState {

        private long count = 0;

        public CountState(BufferAllocator allocator, Field field, PhysicalCount accumulator) {
            super(allocator, field, accumulator);
        }

        @Override
        public boolean needMoreData() throws ComputeException {
            return true;
        }

        @Override
        public void close() throws ComputeException {}

        @Override
        protected FieldVector evaluateImpl() throws ComputeException {
            FieldVector result = field.createVector(allocator);
            writeValue(result.getMinorType().getNewFieldWriter(result));
            return result;
        }

        @Override
        protected void accumulateImpl(ValueVector vector) throws ComputeException {
            // 总元素数量 - 空值数量 = 非空值数量
            count += vector.getValueCount() - vector.getNullCount();
        }

        protected void writeValue(FieldWriter writer) {
            writer.writeBigInt(count);
        }
    }
}
