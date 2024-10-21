package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.unary;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.NotAllowArgumentTypeException;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;

public class PhysicalFirstValue extends UnaryAccumulator {

  public static final String NAME = "first_value";

  public PhysicalFirstValue() {
    super(NAME);
  }

  @Override
  protected UnaryState initializeImpl(BufferAllocator allocator, Field field)
      throws ComputeException {
    Types.MinorType minorType = Types.getMinorTypeForArrowType(field.getType());
    switch (minorType) {
      case INT:
        return new IntFirstState(allocator, field, this);
      case BIGINT:
        return new BigIntFirstState(allocator, field, this);
      case FLOAT4:
        return new Float4FirstState(allocator, field, this);
      case FLOAT8:
        return new Float8FirstState(allocator, field, this);
      case VARCHAR:
        return new VarCharFirstState(allocator, field, this);
      case BIT:
        return new BooleanFirstState(allocator, field, this);
      default:
        throw new NotAllowArgumentTypeException(this, 0, minorType);
    }
  }

  protected abstract static class FirstState extends UnaryState {

    protected boolean foundFirst = false;

    protected FirstState(BufferAllocator allocator, Field field, PhysicalFirstValue accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    public boolean needMoreData() throws ComputeException {
      return !foundFirst;
    }

    @Override
    public void close() throws ComputeException {}

    @Override
    protected FieldVector evaluateImpl() throws ComputeException {
      if (!foundFirst) {
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

  protected static class IntFirstState extends FirstState {
    private Integer first;

    public IntFirstState(BufferAllocator allocator, Field field, PhysicalFirstValue accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      IntVector intVector = (IntVector) vector;
      if (foundFirst) {
        return;
      }
      for (int i = 0; i < intVector.getValueCount(); i++) {
        if (!intVector.isNull(i)) {
          first = intVector.get(i);
          foundFirst = true;
          break;
        }
      }
    }

    @Override
    protected void writeValue(FieldWriter writer) {
      writer.writeInt(first);
    }
  }

  protected static class BigIntFirstState extends FirstState {
    private Long first;

    public BigIntFirstState(BufferAllocator allocator, Field field, PhysicalFirstValue accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      BigIntVector bigIntVector = (BigIntVector) vector;
      if (foundFirst) {
        return;
      }
      for (int i = 0; i < bigIntVector.getValueCount(); i++) {
        if (!bigIntVector.isNull(i)) {
          first = bigIntVector.get(i);
          foundFirst = true;
          break;
        }
      }
    }

    @Override
    protected void writeValue(FieldWriter writer) {
      writer.writeBigInt(first);
    }
  }

  protected static class Float4FirstState extends FirstState {
    private Float first;

    public Float4FirstState(BufferAllocator allocator, Field field, PhysicalFirstValue accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      Float4Vector float4Vector = (Float4Vector) vector;
      if (foundFirst) {
        return;
      }
      for (int i = 0; i < float4Vector.getValueCount(); i++) {
        if (!float4Vector.isNull(i)) {
          first = float4Vector.get(i);
          foundFirst = true;
          break;
        }
      }
    }

    @Override
    protected void writeValue(FieldWriter writer) {
      writer.writeFloat4(first);
    }
  }

  protected static class Float8FirstState extends FirstState {
    private Double first;

    public Float8FirstState(BufferAllocator allocator, Field field, PhysicalFirstValue accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      Float8Vector float8Vector = (Float8Vector) vector;
      if (foundFirst) {
        return;
      }
      for (int i = 0; i < float8Vector.getValueCount(); i++) {
        if (!float8Vector.isNull(i)) {
          first = float8Vector.get(i);
          foundFirst = true;
          break;
        }
      }
    }

    @Override
    protected void writeValue(FieldWriter writer) {
      writer.writeFloat8(first);
    }
  }

  protected static class VarCharFirstState extends FirstState {
    private ArrowBuf first;

    public VarCharFirstState(BufferAllocator allocator, Field field, PhysicalFirstValue accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      VarCharVector varCharVector = (VarCharVector) vector;
      if (foundFirst) {
        return;
      }
      for (int i = 0; i < varCharVector.getValueCount(); i++) {
        if (!varCharVector.isNull(i)) {
          // 获取非空值并将其转换为 ArrowBuf
          byte[] text = varCharVector.get(i);
          int length = text.length;
          first = allocator.buffer(length);
          first.setBytes(0, text, 0, length);
          foundFirst = true;
          break;
        }
      }
    }

    @Override
    protected void writeValue(FieldWriter writer) {
      if (first != null) {
        writer.writeVarChar(0, (int) first.writerIndex(), first);
      } else {
        // 如果没有找到任何非空值，则写入一个空字符串
        writer.writeVarChar(0, 0, null);
      }
    }

    @Override
    public void close() throws ComputeException {
      if (first != null) {
        first.close();
      }
      super.close();
    }
  }

  protected static class BooleanFirstState extends FirstState {
    private Boolean first;

    public BooleanFirstState(BufferAllocator allocator, Field field, PhysicalFirstValue accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      BitVector bitVector = (BitVector) vector;
      if (foundFirst) {
        return;
      }
      for (int i = 0; i < bitVector.getValueCount(); i++) {
        if (!bitVector.isNull(i)) {
          first = bitVector.get(i) == 1;
          foundFirst = true;
          break;
        }
      }
    }

    @Override
    protected void writeValue(FieldWriter writer) {
      writer.writeBit(first ? 1 : 0);
    }
  }
}
