package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.unary;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.NotAllowArgumentTypeException;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;

public class PhysicalMax extends UnaryAccumulator {

  public static final String NAME = "max";

  public PhysicalMax() {
    super(NAME);
  }

  @Override
  protected UnaryState initializeImpl(BufferAllocator allocator, Field field)
      throws ComputeException {
    Types.MinorType minorType = Types.getMinorTypeForArrowType(field.getType());
    switch (minorType) {
      case INT:
        return new IntMaxState(allocator, field, this);
      case BIGINT:
        return new BigIntMaxState(allocator, field, this);
      case FLOAT4:
        return new Float4MaxState(allocator, field, this);
      case FLOAT8:
        return new Float8MaxState(allocator, field, this);
      case VARCHAR:
        return new StringMaxState(allocator, field, this);
      default:
        throw new NotAllowArgumentTypeException(this, 0, minorType);
    }
  }

  protected abstract static class MaxState extends UnaryState {

    protected MaxState(BufferAllocator allocator, Field field, PhysicalMax accumulator) {
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

  protected static class IntMaxState extends MaxState {

    private int max = Integer.MIN_VALUE;

    public IntMaxState(BufferAllocator allocator, Field field, PhysicalMax accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      IntVector intVector = (IntVector) vector;
      for (int i = 0; i < intVector.getValueCount(); i++) {
        if (!intVector.isNull(i)) {
          max = Math.max(max, intVector.get(i));
        }
      }
    }

    @Override
    protected void writeValue(FieldWriter writer) {
      writer.writeInt(max);
    }
  }

  protected static class BigIntMaxState extends MaxState {

    private long max = Long.MIN_VALUE;

    public BigIntMaxState(BufferAllocator allocator, Field field, PhysicalMax accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      BigIntVector bigIntVector = (BigIntVector) vector;
      for (int i = 0; i < bigIntVector.getValueCount(); i++) {
        if (!bigIntVector.isNull(i)) {
          max = Math.max(max, bigIntVector.get(i));
        }
      }
    }

    @Override
    protected void writeValue(FieldWriter writer) {
      writer.writeBigInt(max);
    }
  }

  protected static class Float4MaxState extends MaxState {

    private float max = -Float.MAX_VALUE;

    public Float4MaxState(BufferAllocator allocator, Field field, PhysicalMax accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      Float4Vector float4Vector = (Float4Vector) vector;
      for (int i = 0; i < float4Vector.getValueCount(); i++) {
        if (!float4Vector.isNull(i)) {
          max = Math.max(max, float4Vector.get(i));
        }
      }
    }

    @Override
    protected void writeValue(FieldWriter writer) {
      writer.writeFloat4(max);
    }
  }

  protected static class Float8MaxState extends MaxState {

    private double max = -Double.MAX_VALUE;

    public Float8MaxState(BufferAllocator allocator, Field field, PhysicalMax accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      Float8Vector float8Vector = (Float8Vector) vector;
      for (int i = 0; i < float8Vector.getValueCount(); i++) {
        if (!float8Vector.isNull(i)) {
          max = Math.max(max, float8Vector.get(i));
        }
      }
    }

    @Override
    protected void writeValue(FieldWriter writer) {
      writer.writeFloat8(max);
    }
  }

  protected static class StringMaxState extends MaxState {

    private ArrowBuf max;
    private byte[] maxStr = null;

    public StringMaxState(BufferAllocator allocator, Field field, PhysicalMax accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      VarCharVector varCharVector = (VarCharVector) vector;
      for (int i = 0; i < varCharVector.getValueCount(); i++) {
        if (!varCharVector.isNull(i)) {
          byte[] current = varCharVector.get(i);
          if (maxStr == null || new String(current).compareTo(new String(maxStr)) > 0) { // 使用 compareTo 进行比较
            maxStr = current;
          }
        }
      }
    }

    @Override
    protected void writeValue(FieldWriter writer) {
      if (maxStr != null) {
        int length = maxStr.length;
        max = allocator.buffer(length);
        max.setBytes(0, maxStr, 0, length);
        writer.writeVarChar(0, (int) max.writerIndex(), max);
      } else {
        writer.writeNull();
      }
    }
  }
}
