/*
 * IGinX - the polystore system with high performance
 * Copyright (C) Tsinghua University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.scalar;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.Arity;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ArityException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.compute.util.ComputeException;
import java.util.Objects;
import javax.annotation.WillNotClose;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

public abstract class AbstractFunction implements ScalarFunction {

  private final String name;
  private final Arity arity;

  protected AbstractFunction(String name, Arity arity) {
    this.name = Objects.requireNonNull(name);
    this.arity = Objects.requireNonNull(arity);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public FieldVector invoke(
      @WillNotClose BufferAllocator allocator, @WillNotClose VectorSchemaRoot input)
      throws ComputeException {
    int vectorCount = input.getFieldVectors().size();
    if (!arity.checkArity(vectorCount)) {
      throw new ArityException(this, arity, vectorCount);
    }
    return invokeImpl(allocator, input);
  }

  @Override
  public String toString() {
    return getName();
  }

  protected abstract FieldVector invokeImpl(
      @WillNotClose BufferAllocator allocator, @WillNotClose VectorSchemaRoot input)
      throws ComputeException;
}