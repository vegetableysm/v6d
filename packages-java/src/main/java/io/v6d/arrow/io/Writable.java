// Code generated by alibaba/fastFFI. DO NOT EDIT.
//
package io.v6d.arrow.io;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import io.v6d.arrow.Buffer;
import io.v6d.arrow.Status;
import io.v6d.std.shared_ptr;
import java.lang.Class;
import java.lang.ClassNotFoundException;
import java.lang.IllegalAccessException;
import java.lang.InstantiationException;
import java.lang.Long;
import java.lang.NoSuchMethodException;
import java.lang.reflect.InvocationTargetException;

@FFITypeAlias("arrow::io::Writable")
@FFIGen
@CXXHead(
        system = "arrow/io/interfaces.h"
)
public interface Writable extends CXXPointer {
    @CXXValue
    Status Write(
            @CXXReference @FFITypeAlias("const std::shared_ptr<arrow::Buffer>") shared_ptr<Buffer> data);

    @CXXValue
    Status Flush();

    static Writable cast(final long __foreign_address) {
        try {
            Class<Writable> clz = (Class<Writable>) FFITypeFactory.getType(FFITypeFactory.getFFITypeName(Writable.class, true));
            return clz.getConstructor(Long.TYPE).newInstance(__foreign_address);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            return null;
        }
    }

    static Writable cast(final FFIPointer __foreign_pointer) {
        return Writable.cast(__foreign_pointer.getAddress());
    }
}
