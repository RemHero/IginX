package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.unary;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.NotAllowArgumentTypeException;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;

public class PhysicalMin extends UnaryAccumulator {

  public static final String NAME = "min";

  public PhysicalMin() {
    super(NAME);
  }

  @Override
  protected UnaryState initializeImpl(BufferAllocator allocator, Field field)
      throws ComputeException {
    Types.MinorType minorType = Types.getMinorTypeForArrowType(field.getType());
    switch (minorType) {
      case INT:
        return new IntMinState(allocator, field, this);
      case BIGINT:
        return new BigIntMinState(allocator, field, this);
      case FLOAT4:
        return new Float4MinState(allocator, field, this);
      case FLOAT8:
        return new Float8MinState(allocator, field, this);
      default:
        throw new NotAllowArgumentTypeException(this, 0, minorType);
    }
  }

  protected abstract static class MinState extends UnaryState {

    protected MinState(BufferAllocator allocator, Field field, PhysicalMin accumulator) {
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

    protected abstract void writeValue(FieldWriter writer);
  }

  protected static class IntMinState extends MinState {

    private int min = Integer.MAX_VALUE;

    public IntMinState(BufferAllocator allocator, Field field, PhysicalMin accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      IntVector intVector = (IntVector) vector;
      for (int i = 0; i < intVector.getValueCount(); i++) {
        if (!intVector.isNull(i)) {
          min = Math.min(min, intVector.get(i));
        }
      }
    }

    @Override
    protected void writeValue(FieldWriter writer) {
      writer.writeInt(min);
    }
  }

  protected static class BigIntMinState extends MinState {

    private long min = Long.MAX_VALUE;

    public BigIntMinState(BufferAllocator allocator, Field field, PhysicalMin accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      BigIntVector bigIntVector = (BigIntVector) vector;
      for (int i = 0; i < bigIntVector.getValueCount(); i++) {
        if (!bigIntVector.isNull(i)) {
          min = Math.min(min, bigIntVector.get(i));
        }
      }
    }

    @Override
    protected void writeValue(FieldWriter writer) {
      writer.writeBigInt(min);
    }
  }

  protected static class Float4MinState extends MinState {

    private float min = Float.POSITIVE_INFINITY;

    public Float4MinState(BufferAllocator allocator, Field field, PhysicalMin accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      Float4Vector float4Vector = (Float4Vector) vector;
      for (int i = 0; i < float4Vector.getValueCount(); i++) {
        if (!float4Vector.isNull(i)) {
          min = Math.min(min, float4Vector.get(i));
        }
      }
    }

    @Override
    protected void writeValue(FieldWriter writer) {
      writer.writeFloat4(min);
    }
  }

  protected static class Float8MinState extends MinState {

    private double min = Double.POSITIVE_INFINITY;

    public Float8MinState(BufferAllocator allocator, Field field, PhysicalMin accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      Float8Vector float8Vector = (Float8Vector) vector;
      for (int i = 0; i < float8Vector.getValueCount(); i++) {
        if (!float8Vector.isNull(i)) {
          min = Math.min(min, float8Vector.get(i));
        }
      }
    }

    @Override
    protected void writeValue(FieldWriter writer) {
      writer.writeFloat8(min);
    }
  }

  protected static class StringMaxState extends MinState {

    private ArrowBuf min;
    private byte[] minStr = null;

    public StringMaxState(BufferAllocator allocator, Field field, PhysicalMin accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      VarCharVector varCharVector = (VarCharVector) vector;
      for (int i = 0; i < varCharVector.getValueCount(); i++) {
        if (!varCharVector.isNull(i)) {
          byte[] current = varCharVector.get(i);
          if (minStr == null || new String(current).compareTo(new String(minStr)) > 0) { // 使用 compareTo 进行比较
            minStr = current;
          }
        }
      }
    }

    @Override
    protected void writeValue(FieldWriter writer) {
      if (minStr != null) {
        int length = minStr.length;
        min = allocator.buffer(length);
        min.setBytes(0, minStr, 0, length);
        writer.writeVarChar(0, (int) min.writerIndex(), min);
      } else {
        writer.writeNull();
      }
    }
  }
}
