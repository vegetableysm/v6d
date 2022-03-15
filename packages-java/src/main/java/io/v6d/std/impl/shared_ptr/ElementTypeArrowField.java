// Code generated by alibaba/fastFFI. DO NOT EDIT.
//
package io.v6d.std.impl.shared_ptr;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXPointer;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIExpr;
import com.alibaba.fastffi.FFIFactory;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFIPointer;
import com.alibaba.fastffi.FFITypeAlias;
import com.alibaba.fastffi.FFITypeFactory;
import io.v6d.arrow.DataType;
import io.v6d.arrow.Field;
import io.v6d.arrow.KeyValueMetadata;
import io.v6d.arrow.Result;
import io.v6d.std.shared_ptr;
import io.v6d.std.string;
import io.v6d.std.vector;
import java.lang.Class;
import java.lang.ClassNotFoundException;
import java.lang.IllegalAccessException;
import java.lang.InstantiationException;
import java.lang.Long;
import java.lang.NoSuchMethodException;
import java.lang.reflect.InvocationTargetException;

@FFITypeAlias("std::shared_ptr<arrow::Field>::element_type")
@FFIGen
@CXXHead(
        system = "__memory/shared_ptr.h"
)
public interface ElementTypeArrowField extends CXXPointer {
    @FFIExpr("{0}")
    Field get();

    @CXXValue
    @FFITypeAlias("std::shared_ptr<const arrow::KeyValueMetadata>")
    shared_ptr<KeyValueMetadata> metadata();

    boolean HasMetadata();

    @CXXValue
    @FFITypeAlias("std::shared_ptr<arrow::Field>")
    shared_ptr<Field> WithMetadata(
            @CXXReference @FFITypeAlias("const std::shared_ptr<const arrow::KeyValueMetadata>") shared_ptr<KeyValueMetadata> metadata);

    @CXXValue
    @FFITypeAlias("std::shared_ptr<arrow::Field>")
    shared_ptr<Field> WithMergedMetadata(
            @CXXReference @FFITypeAlias("const std::shared_ptr<const arrow::KeyValueMetadata>") shared_ptr<KeyValueMetadata> metadata);

    @CXXValue
    @FFITypeAlias("std::shared_ptr<arrow::Field>")
    shared_ptr<Field> RemoveMetadata();

    @CXXValue
    @FFITypeAlias("std::shared_ptr<arrow::Field>")
    shared_ptr<Field> WithType(
            @CXXReference @FFITypeAlias("const std::shared_ptr<arrow::DataType>") shared_ptr<DataType> type);

    @CXXValue
    @FFITypeAlias("std::shared_ptr<arrow::Field>")
    shared_ptr<Field> WithName(@CXXReference string name);

    @CXXValue
    @FFITypeAlias("std::shared_ptr<arrow::Field>")
    shared_ptr<Field> WithNullable(boolean nullable);

    @CXXValue
    @FFITypeAlias("arrow::Result<std::shared_ptr<arrow::Field>>")
    Result<shared_ptr<Field>> MergeWith(@CXXReference Field other,
            @CXXValue Field.MergeOptions options);

    @CXXValue
    @FFITypeAlias("arrow::Result<std::shared_ptr<arrow::Field>>")
    Result<shared_ptr<Field>> MergeWith(
            @CXXReference @FFITypeAlias("const std::shared_ptr<arrow::Field>") shared_ptr<Field> other,
            @CXXValue Field.MergeOptions options);

    @CXXValue
    @FFITypeAlias("std::vector<std::shared_ptr<arrow::Field>>")
    vector<shared_ptr<Field>> Flatten();

    boolean Equals(@CXXReference Field other, boolean check_metadata);

    boolean Equals(
            @CXXReference @FFITypeAlias("const std::shared_ptr<arrow::Field>") shared_ptr<Field> other,
            boolean check_metadata);

    boolean IsCompatibleWith(@CXXReference Field other);

    boolean IsCompatibleWith(
            @CXXReference @FFITypeAlias("const std::shared_ptr<arrow::Field>") shared_ptr<Field> other);

    @CXXValue
    string ToString(boolean show_metadata);

    @CXXReference
    string name();

    @CXXReference
    @FFITypeAlias("const std::shared_ptr<arrow::DataType>")
    shared_ptr<DataType> type();

    boolean nullable();

    @CXXValue
    @FFITypeAlias("std::shared_ptr<arrow::Field>")
    shared_ptr<Field> Copy();

    static ElementTypeArrowField cast(final long __foreign_address) {
        try {
            Class<ElementTypeArrowField> clz = (Class<ElementTypeArrowField>) FFITypeFactory.getType(FFITypeFactory.getFFITypeName(ElementTypeArrowField.class, true));
            return clz.getConstructor(Long.TYPE).newInstance(__foreign_address);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            return null;
        }
    }

    static ElementTypeArrowField cast(final FFIPointer __foreign_pointer) {
        return ElementTypeArrowField.cast(__foreign_pointer.getAddress());
    }

    static Factory getFactory() {
        return FFITypeFactory.getFactory(FFITypeFactory.getFFITypeName(ElementTypeArrowField.class, true));
    }

    static ElementTypeArrowField create(@CXXValue string name,
            @CXXValue @FFITypeAlias("std::shared_ptr<arrow::DataType>") shared_ptr<DataType> type,
            boolean nullable,
            @CXXValue @FFITypeAlias("std::shared_ptr<const arrow::KeyValueMetadata>") shared_ptr<KeyValueMetadata> metadata) {
        return ElementTypeArrowField.getFactory().create(name, type, nullable, metadata);
    }

    @FFIFactory
    @CXXHead(
            system = "__memory/shared_ptr.h"
    )
    interface Factory {
        ElementTypeArrowField create(@CXXValue string name,
                @CXXValue @FFITypeAlias("std::shared_ptr<arrow::DataType>") shared_ptr<DataType> type,
                boolean nullable,
                @CXXValue @FFITypeAlias("std::shared_ptr<const arrow::KeyValueMetadata>") shared_ptr<KeyValueMetadata> metadata);
    }
}
