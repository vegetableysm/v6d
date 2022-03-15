// Code generated by alibaba/fastFFI. DO NOT EDIT.
//
package io.v6d.vineyard;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFILibrary;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import io.v6d.arrow.Schema;
import io.v6d.std.shared_ptr;
import io.v6d.std.unique_ptr;
import io.v6d.std.vector;
import java.lang.Class;
import java.lang.ClassNotFoundException;
import java.lang.IllegalAccessException;
import java.lang.InstantiationException;
import java.lang.Long;
import java.lang.NoSuchMethodException;
import java.lang.reflect.InvocationTargetException;

@FFITypeAlias("vineyard::RecordBatch")
@FFIGen
@CXXHead("basic/ds/arrow.vineyard.h")
public interface RecordBatch extends Registered<RecordBatch>, FFIPointer {
    @FFINameAlias("Construct")
    void Construct_1(@CXXReference ObjectMeta meta);

    @FFINameAlias("PostConstruct")
    void PostConstruct_1(@CXXReference ObjectMeta meta);

    @CXXValue
    @FFITypeAlias("std::shared_ptr<arrow::RecordBatch>")
    shared_ptr<io.v6d.arrow.RecordBatch> GetRecordBatch();

    @CXXValue
    @FFITypeAlias("std::shared_ptr<arrow::Schema>")
    shared_ptr<Schema> schema();

    long num_columns();

    long num_rows();

    @CXXReference
    @FFITypeAlias("const std::vector<std::shared_ptr<vineyard::Object>>")
    vector<shared_ptr<Object>> columns();

    static RecordBatch cast(final long __foreign_address) {
        try {
            Class<RecordBatch> clz = (Class<RecordBatch>) FFITypeFactory.getType(FFITypeFactory.getFFITypeName(RecordBatch.class, true));
            return clz.getConstructor(Long.TYPE).newInstance(__foreign_address);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            return null;
        }
    }

    static RecordBatch cast(final FFIPointer __foreign_pointer) {
        return RecordBatch.cast(__foreign_pointer.getAddress());
    }

    @FFIGen
    @FFILibrary(
            value = "vineyard::RecordBatch",
            namespace = "vineyard::RecordBatch"
    )
    @CXXHead("basic/ds/arrow.vineyard.h")
    interface Library {
        Library INSTANCE = FFITypeFactory.getLibrary(Library.class);

        @CXXValue
        @FFITypeAlias("std::unique_ptr<vineyard::Object>")
        unique_ptr<Object> Create();
    }
}
