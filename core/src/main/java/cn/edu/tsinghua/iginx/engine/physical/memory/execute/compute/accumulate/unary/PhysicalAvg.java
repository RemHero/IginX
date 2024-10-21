package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.accumulate.unary;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.NotAllowArgumentTypeException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.writer.FieldWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;

public class PhysicalAvg extends UnaryAccumulator {

  public static final String NAME = "avg";

  public PhysicalAvg() {
    super(NAME);
  }

  @Override
  protected UnaryState initializeImpl(BufferAllocator allocator, Field field)
      throws ComputeException {
    Types.MinorType minorType = Types.getMinorTypeForArrowType(field.getType());
    switch (minorType) {
      case INT:
        return new IntAvgState(allocator, field, this);
      case BIGINT:
        return new BigIntAvgState(allocator, field, this);
      case FLOAT4:
        return new Float4AvgState(allocator, field, this);
      case FLOAT8:
        return new Float8AvgState(allocator, field, this);
      default:
        throw new NotAllowArgumentTypeException(this, 0, minorType);
    }
  }

  protected abstract static class AvgState extends UnaryState {

    protected long count; // Number of non-null elements

    protected AvgState(BufferAllocator allocator, Field field, PhysicalAvg accumulator) {
      super(allocator, field, accumulator);
      this.count = 0;
    }

    @Override
    public boolean needMoreData() throws ComputeException {
      return true;
    }

    @Override
    public void close() throws ComputeException {}

    @Override
    protected FieldVector evaluateImpl() throws ComputeException {
      if (count == 0) {
        FieldVector result = field.createVector(allocator);
        result.setNull(0); // If there's no data, the average is null
        return result;
      }
      FieldVector result = field.createVector(allocator);
      writeValue(result.getMinorType().getNewFieldWriter(result));
      return result;
    }

    protected abstract double getSum();

    protected void writeValue(FieldWriter writer) {
      if (count > 0) {
        writer.writeFloat8(getSum() / count);
      } else {
        writer.writeNull();
      }
    }
  }

  protected static class IntAvgState extends AvgState {

    private long sum = 0;

    public IntAvgState(BufferAllocator allocator, Field field, PhysicalAvg accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      IntVector intVector = (IntVector) vector;
      for (int i = 0; i < intVector.getValueCount(); i++) {
        if (!intVector.isNull(i)) {
          sum += intVector.get(i);
          count++;
        }
      }
    }

    @Override
    protected double getSum() {
      return sum;
    }
  }

  protected static class BigIntAvgState extends AvgState {

    private long sum = 0;

    public BigIntAvgState(BufferAllocator allocator, Field field, PhysicalAvg accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      BigIntVector bigIntVector = (BigIntVector) vector;
      for (int i = 0; i < bigIntVector.getValueCount(); i++) {
        if (!bigIntVector.isNull(i)) {
          sum += bigIntVector.get(i);
          count++;
        }
      }
    }

    @Override
    protected double getSum() {
      return sum;
    }
  }

  protected static class Float4AvgState extends AvgState {

    private double sum = 0;

    public Float4AvgState(BufferAllocator allocator, Field field, PhysicalAvg accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      Float4Vector float4Vector = (Float4Vector) vector;
      for (int i = 0; i < float4Vector.getValueCount(); i++) {
        if (!float4Vector.isNull(i)) {
          sum += float4Vector.get(i);
          count++;
        }
      }
    }

    @Override
    protected double getSum() {
      return sum;
    }
  }

  protected static class Float8AvgState extends AvgState {

    private double sum = 0;

    public Float8AvgState(BufferAllocator allocator, Field field, PhysicalAvg accumulator) {
      super(allocator, field, accumulator);
    }

    @Override
    protected void accumulateImpl(ValueVector vector) throws ComputeException {
      Float8Vector float8Vector = (Float8Vector) vector;
      for (int i = 0; i < float8Vector.getValueCount(); i++) {
        if (!float8Vector.isNull(i)) {
          sum += float8Vector.get(i);
          count++;
        }
      }
    }

    @Override
    protected double getSum() {
      return sum;
    }
  }
}
