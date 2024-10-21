package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.unary;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.NotAllowArgumentTypeException;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;

public class PhysicalLastValue extends UnaryAccumulator {

  public static final String NAME = "last_value";

  public PhysicalLastValue() {
    super(NAME);
  }

  @Override
  protected UnaryState initializeImpl(BufferAllocator allocator, Field field)
      throws ComputeException {
    Types.MinorType minorType = Types.getMinorTypeForArrowType(field.getType());
    switch (minorType) {
      case INT:
        return new IntLastState(allocator, field, this);
      case BIGINT:
        return new BigIntLastState(allocator, field, this);
      case FLOAT4:
        return new Float4LastState(allocator, field, this);
      case FLOAT8:
        return new Float8LastState(allocator, field, this);
      case VARCHAR:
        return new VarCharLastState(allocator, field, this);
      case BIT:
        return new BooleanFirstState(allocator, field, this);
      default:
        throw new NotAllowArgumentTypeException(this, 0, minorType);
    }
  }

  protected abstract static class LastState extends UnaryState {

    protected boolean foundLast = false;

    protected LastState(BufferAllocator allocator, Field field, PhysicalLastValue accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    public boolean needMoreData() throws ComputeException {
      return !foundLast;
    }

    @Override
    public void close() throws ComputeException {}

    @Override
    protected FieldVector evaluateImpl() throws ComputeException {
      if (!foundLast) {
        // 如果没有找到非空值，则创建一个包含单个 null 值的向量
        FieldVector result = field.createVector(allocator);
        result.setInitialCapacity(1);
        result.setNull(0);
        result.setValueCount(1);
        return result;
      } else {
        FieldVector result = field.createVector(allocator);
        writeValue(result.getMinorType().getNewFieldWriter(result));
        return result;
      }
    }

    protected abstract void accumulateImpl(ValueVector vector) throws ComputeException;

    protected abstract void writeValue(FieldWriter writer);
  }

  protected static class IntLastState extends LastState {
    private Integer last;

    public IntLastState(BufferAllocator allocator, Field field, PhysicalLastValue accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      IntVector intVector = (IntVector) vector;
      for (int i = intVector.getValueCount() - 1; i >= 0; i--) {
        if (!intVector.isNull(i)) {
          last = intVector.get(i);
          foundLast = true;
          break;
        }
      }
    }

    @Override
    protected void writeValue(FieldWriter writer) {
      writer.writeInt(last);
    }
  }

  protected static class BigIntLastState extends LastState {
    private Long last;

    public BigIntLastState(BufferAllocator allocator, Field field, PhysicalLastValue accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      BigIntVector bigIntVector = (BigIntVector) vector;
      for (int i = bigIntVector.getValueCount() - 1; i >= 0; i--) {
        if (!bigIntVector.isNull(i)) {
          last = bigIntVector.get(i);
          foundLast = true;
          break;
        }
      }
    }

    @Override
    protected void writeValue(FieldWriter writer) {
      writer.writeBigInt(last);
    }
  }

  protected static class Float4LastState extends LastState {
    private Float last;

    public Float4LastState(BufferAllocator allocator, Field field, PhysicalLastValue accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      Float4Vector float4Vector = (Float4Vector) vector;
      for (int i = float4Vector.getValueCount() - 1; i >= 0; i--) {
        if (!float4Vector.isNull(i)) {
          last = float4Vector.get(i);
          foundLast = true;
          break;
        }
      }
    }

    @Override
    protected void writeValue(FieldWriter writer) {
      writer.writeFloat4(last);
    }
  }

  protected static class Float8LastState extends LastState {
    private Double last;

    public Float8LastState(BufferAllocator allocator, Field field, PhysicalLastValue accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      Float8Vector float8Vector = (Float8Vector) vector;
      for (int i = float8Vector.getValueCount() - 1; i >= 0; i--) {
        if (!float8Vector.isNull(i)) {
          last = float8Vector.get(i);
          foundLast = true;
          break;
        }
      }
    }

    @Override
    protected void writeValue(FieldWriter writer) {
      writer.writeFloat8(last);
    }
  }

  protected static class VarCharLastState extends LastState {
    private ArrowBuf last;

    public VarCharLastState(BufferAllocator allocator, Field field, PhysicalLastValue accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      VarCharVector varCharVector = (VarCharVector) vector;
      for (int i = varCharVector.getValueCount() - 1; i >= 0; i--) {
        if (!varCharVector.isNull(i)) {
          byte[] text = varCharVector.get(i);
          int length = text.length;
          last = allocator.buffer(length);
          last.setBytes(0, text, 0, length);
          foundLast = true;
          break;
        }
      }
    }

    @Override
    protected void writeValue(FieldWriter writer) {
      if (last != null) {
        writer.writeVarChar(0, (int) last.writerIndex(), last);
      } else {
        writer.writeVarChar(0, 0, null); // 写入空字符串
      }
    }

    @Override
    public void close() throws ComputeException {
      if (last != null) {
        last.close();
      }
      super.close();
    }
  }

  protected static class BooleanFirstState extends LastState {
    private Boolean last;

    public BooleanFirstState(BufferAllocator allocator, Field field, PhysicalLastValue accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      BitVector bitVector = (BitVector) vector;
      if (foundLast) {
        return;
      }
      for (int i = 0; i < bitVector.getValueCount(); i++) {
        if (!bitVector.isNull(i)) {
          last = bitVector.get(i) == 1;
          foundLast = true;
          break;
        }
      }
    }

    @Override
    protected void writeValue(FieldWriter writer) {
      writer.writeBit(last ? 1 : 0);
    }
  }
}
