// Code generated by alibaba/fastFFI. DO NOT EDIT.
//
package io.v6d.arrow;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFILibrary;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import io.v6d.std.enable_shared_from_this;
import io.v6d.std.shared_ptr;
import java.lang.Class;
import java.lang.ClassNotFoundException;
import java.lang.IllegalAccessException;
import java.lang.InstantiationException;
import java.lang.Long;
import java.lang.NoSuchMethodException;
import java.lang.reflect.InvocationTargetException;

@FFITypeAlias("arrow::MemoryManager")
@FFIGen
@CXXHead(
        system = "arrow/device.h"
)
@CXXHead("arrow/result.h")
public interface MemoryManager extends enable_shared_from_this<MemoryManager>, CXXPointer {
    @CXXReference
    @FFITypeAlias("const std::shared_ptr<arrow::Device>")
    shared_ptr<Device> device();

    boolean is_cpu();

    static MemoryManager cast(final long __foreign_address) {
        try {
            Class<MemoryManager> clz = (Class<MemoryManager>) FFITypeFactory.getType(FFITypeFactory.getFFITypeName(MemoryManager.class, true));
            return clz.getConstructor(Long.TYPE).newInstance(__foreign_address);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            return null;
        }
    }

    static MemoryManager cast(final FFIPointer __foreign_pointer) {
        return MemoryManager.cast(__foreign_pointer.getAddress());
    }

    @FFIGen
    @FFILibrary(
            value = "arrow::MemoryManager",
            namespace = "arrow::MemoryManager"
    )
    @CXXHead(
            system = "arrow/device.h"
    )
    @CXXHead("arrow/result.h")
    interface Library {
        Library INSTANCE = FFITypeFactory.getLibrary(Library.class);

        @CXXValue
        @FFITypeAlias("arrow::Result<std::shared_ptr<arrow::Buffer>>")
        Result<shared_ptr<Buffer>> CopyBuffer(
                @CXXReference @FFITypeAlias("const std::shared_ptr<arrow::Buffer>") shared_ptr<Buffer> source,
                @CXXReference @FFITypeAlias("const std::shared_ptr<arrow::MemoryManager>") shared_ptr<MemoryManager> to);

        @CXXValue
        @FFITypeAlias("arrow::Result<std::shared_ptr<arrow::Buffer>>")
        Result<shared_ptr<Buffer>> ViewBuffer(
                @CXXReference @FFITypeAlias("const std::shared_ptr<arrow::Buffer>") shared_ptr<Buffer> source,
                @CXXReference @FFITypeAlias("const std::shared_ptr<arrow::MemoryManager>") shared_ptr<MemoryManager> to);
    }
}
