/** Copyright 2020-2023 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef MODULES_GRAPH_FRAGMENT_PROPERTY_GRAPH_TYPES_H_
#define MODULES_GRAPH_FRAGMENT_PROPERTY_GRAPH_TYPES_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "grape/config.h"
#include "grape/utils/vertex_array.h"

#include "basic/ds/arrow.h"
#include "common/util/arrow.h"
#include "graph/fragment/varint_impl.h"
#include "graph/fragment/GroupVarint.h"

namespace vineyard {

using fid_t = grape::fid_t;

template <typename T>
struct InternalType {
  using type = T;
  using vineyard_array_type = vineyard::NumericArray<T>;
  using vineyard_builder_type = vineyard::NumericArrayBuilder<T>;
};

template <>
struct InternalType<std::string> {
  using type = arrow_string_view;
  using vineyard_array_type = vineyard::LargeStringArray;
  using vineyard_builder_type = vineyard::LargeStringArrayBuilder;
};

template <>
struct InternalType<arrow_string_view> {
  using type = arrow_string_view;
  using vineyard_array_type = vineyard::LargeStringArray;
  using vineyard_builder_type = vineyard::LargeStringArrayBuilder;
};

namespace property_graph_types {

using OID_TYPE = int64_t;
using EID_TYPE = uint64_t;
using VID_TYPE = uint64_t;
// using VID_TYPE = uint32_t;

using PROP_ID_TYPE = int;
using LABEL_ID_TYPE = int;

}  // namespace property_graph_types

// Hardcoded the max vertex label num to 128
constexpr int MAX_VERTEX_LABEL_NUM = 128;

static inline int num_to_bitwidth(int num) {
  if (num <= 2) {
    return 1;
  }
  int max = num - 1;
  int width = 0;
  while (max) {
    ++width;
    max >>= 1;
  }
  return width;
}

/**
 * @brief IdParser is designed for parsing the IDs that associated with property
 * graphs
 *
 * @tparam ID_TYPE
 */
template <typename ID_TYPE>
class IdParser {
  using LabelIDT = int;  // LABEL_ID_TYPE

 public:
  IdParser() {}
  ~IdParser() {}

  void Init(fid_t fnum, LabelIDT label_num) {
    CHECK_LE(label_num, MAX_VERTEX_LABEL_NUM);
    int fid_width = num_to_bitwidth(fnum);
    fid_offset_ = (sizeof(ID_TYPE) * 8) - fid_width;
    int label_width = num_to_bitwidth(MAX_VERTEX_LABEL_NUM);
    label_id_offset_ = fid_offset_ - label_width;
    fid_mask_ = ((((ID_TYPE) 1) << fid_width) - (ID_TYPE) 1) << fid_offset_;
    lid_mask_ = (((ID_TYPE) 1) << fid_offset_) - ((ID_TYPE) 1);
    label_id_mask_ = ((((ID_TYPE) 1) << label_width) - (ID_TYPE) 1)
                     << label_id_offset_;
    offset_mask_ = (((ID_TYPE) 1) << label_id_offset_) - (ID_TYPE) 1;
  }

  fid_t GetFid(ID_TYPE v) const { return (v >> fid_offset_); }

  LabelIDT GetLabelId(ID_TYPE v) const {
    return (v & label_id_mask_) >> label_id_offset_;
  }

  int64_t GetOffset(ID_TYPE v) const { return (v & offset_mask_); }

  ID_TYPE GetLid(ID_TYPE v) const { return v & lid_mask_; }

  /**
   * @brief Generate the LID
   */
  ID_TYPE GenerateId(LabelIDT label, int64_t offset) const {
    return (((ID_TYPE) offset) & offset_mask_) |
           ((((ID_TYPE) label) << label_id_offset_) & label_id_mask_);
  }

  /**
   * @brief Generate the GID
   */
  ID_TYPE GenerateId(fid_t fid, LabelIDT label, int64_t offset) const {
    return (((ID_TYPE) offset) & offset_mask_) |
           ((((ID_TYPE) label) << label_id_offset_) & label_id_mask_) |
           ((((ID_TYPE) fid) << fid_offset_) & fid_mask_);
  }

  ID_TYPE offset_mask() const { return offset_mask_; }

 private:
  int fid_offset_;
  int label_id_offset_;
  ID_TYPE fid_mask_;
  ID_TYPE lid_mask_;
  ID_TYPE label_id_mask_;
  ID_TYPE offset_mask_;
};

namespace property_graph_utils {

template <typename VID_T, typename EID_T>
struct NbrUnit {
  using vid_t = VID_T;
  using eid_t = EID_T;

  VID_T vid;
  EID_T eid;
  NbrUnit() = default;
  NbrUnit(VID_T v, EID_T e) : vid(v), eid(e) {}

  grape::Vertex<VID_T> get_neighbor() const {
    return grape::Vertex<VID_T>(vid);
  }
};

template <typename VID_T>
using NbrUnitDefault = NbrUnit<VID_T, property_graph_types::EID_TYPE>;

template <typename DATA_T, typename NBR_T>
class EdgeDataColumn {
 public:
  EdgeDataColumn() = default;

  explicit EdgeDataColumn(std::shared_ptr<arrow::Array> array) {
    if (array->type()->Equals(
            vineyard::ConvertToArrowType<DATA_T>::TypeValue())) {
      data_ = std::dynamic_pointer_cast<ArrowArrayType<DATA_T>>(array)
                  ->raw_values();
    } else {
      data_ = NULL;
    }
  }

  const DATA_T& operator[](const NBR_T& nbr) const { return data_[nbr.eid]; }

  const DATA_T& operator[](const typename NBR_T::eid_t& eid) const {
    return data_[eid];
  }

 private:
  const DATA_T* data_;
};

template <typename NBR_T>
class EdgeDataColumn<std::string, NBR_T> {
 public:
  EdgeDataColumn() = default;

  explicit EdgeDataColumn(std::shared_ptr<arrow::Array> array) {
    if (array->type()->Equals(arrow::large_utf8())) {
      array_ = std::dynamic_pointer_cast<arrow::LargeStringArray>(array);
    } else {
      array_ = nullptr;
    }
  }

  std::string operator[](const NBR_T& nbr) const {
    return array_->GetView(nbr.eid);
  }

  std::string operator[](const typename NBR_T::eid_t& eid) const {
    return array_->GetView(eid);
  }

 private:
  std::shared_ptr<arrow::LargeStringArray> array_;
};

template <typename DATA_T, typename VID_T>
using EdgeDataColumnDefault = EdgeDataColumn<DATA_T, NbrUnitDefault<VID_T>>;

template <typename DATA_T, typename VID_T>
class VertexDataColumn {
 public:
  VertexDataColumn(grape::VertexRange<VID_T> range,
                   std::shared_ptr<arrow::Array> array)
      : range_(range) {
    if (array->type()->Equals(
            vineyard::ConvertToArrowType<DATA_T>::TypeValue())) {
      data_ = std::dynamic_pointer_cast<ArrowArrayType<DATA_T>>(array)
                  ->raw_values() -
              static_cast<ptrdiff_t>(range.begin().GetValue());
    } else {
      data_ = NULL;
    }
  }

  explicit VertexDataColumn(grape::VertexRange<VID_T> range) : range_(range) {
    CHECK_EQ(range.size(), 0);
    data_ = nullptr;
  }

  const DATA_T& operator[](const grape::Vertex<VID_T>& v) const {
    return data_[v.GetValue()];
  }

 private:
  const DATA_T* data_;
  grape::VertexRange<VID_T> range_;
};

template <typename VID_T>
class VertexDataColumn<std::string, VID_T> {
 public:
  VertexDataColumn(grape::VertexRange<VID_T> range,
                   std::shared_ptr<arrow::Array> array)
      : range_(range) {
    if (array->type()->Equals(arrow::large_utf8())) {
      array_ = std::dynamic_pointer_cast<arrow::LargeStringArray>(array);
    } else {
      array_ = nullptr;
    }
  }

  explicit VertexDataColumn(grape::VertexRange<VID_T> range) : range_(range) {
    CHECK_EQ(range.size(), 0);
    array_ = nullptr;
  }

  std::string operator[](const grape::Vertex<VID_T>& v) const {
    return array_->GetView(v.GetValue() - range_.begin().GetValue());
  }

 private:
  grape::VertexRange<VID_T> range_;
  std::shared_ptr<arrow::LargeStringArray> array_;
};

template <typename T>
struct ValueGetter {
  inline static T Value(const void* data, int64_t offset) {
    return reinterpret_cast<const T*>(data)[offset];
  }
};

template <>
struct ValueGetter<std::string> {
  inline static std::string Value(const void* data, int64_t offset) {
    return std::string(
        reinterpret_cast<const arrow::LargeStringArray*>(data)->GetView(
            offset));
  }
};

template <typename VID_T, typename EID_T>
struct Nbr {
 private:
  using vid_t = VID_T;
  using eid_t = EID_T;
  using prop_id_t = property_graph_types::PROP_ID_TYPE;

 public:
  Nbr()
      : nbr_(NULL),
        v_ptr_(nullptr),
        e_ptr_(nullptr),
        encoded_(false),
        edata_arrays_(nullptr) {}
  Nbr(const NbrUnit<VID_T, EID_T>* nbr, const void** edata_arrays)
      : nbr_(nbr),
        v_ptr_(nullptr),
        e_ptr_(nullptr),
        encoded_(false),
        edata_arrays_(edata_arrays) {}
  Nbr(const Nbr& rhs)
      : nbr_(rhs.nbr_),
        v_ptr_(rhs.v_ptr_),
        e_ptr_(rhs.e_ptr_),
        encoded_(false),
        data_(rhs.data_),
        data_valid_(rhs.data_valid_),
        edata_arrays_(rhs.edata_arrays_) {}
  Nbr(Nbr&& rhs)
      : nbr_(std::move(rhs.nbr_)),
        v_ptr_(std::move(rhs.v_ptr_)),
        e_ptr_(std::move(rhs.e_ptr_)),
        encoded_(rhs.encoded_),
        data_(rhs.data_),
        data_valid_(rhs.data_valid_),
        edata_arrays_(rhs.edata_arrays_) {}
  Nbr(const uint8_t* v_ptr, const uint8_t* e_ptr, const void** edata_arrays)
      : nbr_(nullptr),
        v_ptr_(v_ptr),
        e_ptr_(e_ptr),
        encoded_(true),
        edata_arrays_(edata_arrays) {}

  Nbr& operator=(const Nbr& rhs) {
    if (rhs.encoded_) {
      v_ptr_ = rhs.v_ptr_;
      e_ptr_ = rhs.e_ptr_;
      encoded_ = true;
    } else {
      nbr_ = rhs.nbr_;
    }
    edata_arrays_ = rhs.edata_arrays_;
    return *this;
  }

  Nbr& operator=(Nbr&& rhs) {
    if (rhs.encoded_) {
      v_ptr_ = std::move(rhs.v_ptr_);
      e_ptr_ = std::move(rhs.e_ptr_);
      encoded_ = true;
    } else {
      nbr_ = std::move(rhs.nbr_);
    }
    edata_arrays_ = std::move(rhs.edata_arrays_);
    return *this;
  }

  inline void decode() const {
    if (data_valid_) {
      return;
    }
    eid_t eid;
    vid_t vid;
    e_size_ = varint_decode(e_ptr_, eid);
    v_size_ = varint_decode(v_ptr_, vid);
    data_.eid = eid;
    data_.vid = vid;
    data_valid_ = true;
  }

  grape::Vertex<VID_T> neighbor() const {
    if (encoded_) {
      decode();
      return grape::Vertex<VID_T>(data_.vid);
    }
    return grape::Vertex<VID_T>(nbr_->vid);
  }

  grape::Vertex<VID_T> get_neighbor() const {
    if (encoded_) {
      decode();
      return grape::Vertex<VID_T>(data_.vid);
    }
    return grape::Vertex<VID_T>(nbr_->vid);
  }

  EID_T edge_id() const {
    if (encoded_) {
      decode();
      return data_.eid;
    }
    return nbr_->eid;
  }

  template <typename T>
  T get_data(prop_id_t prop_id) const {
    if (encoded_) {
      decode();
      return ValueGetter<T>::Value(edata_arrays_[prop_id], data_.eid);
    }
    return ValueGetter<T>::Value(edata_arrays_[prop_id], nbr_->eid);
  }

  std::string get_str(prop_id_t prop_id) const {
    if (encoded_) {
      decode();
      return ValueGetter<std::string>::Value(edata_arrays_[prop_id], data_.eid);
    }
    return ValueGetter<std::string>::Value(edata_arrays_[prop_id], nbr_->eid);
  }

  double get_double(prop_id_t prop_id) const {
    if (encoded_) {
      decode();
      return ValueGetter<double>::Value(edata_arrays_[prop_id], data_.eid);
    }
    return ValueGetter<double>::Value(edata_arrays_[prop_id], nbr_->eid);
  }

  int64_t get_int(prop_id_t prop_id) const {
    if (encoded_) {
      decode();
      return ValueGetter<int64_t>::Value(edata_arrays_[prop_id], data_.eid);
    }
    return ValueGetter<int64_t>::Value(edata_arrays_[prop_id], nbr_->eid);
  }

  inline const Nbr& operator++() const {
    if (encoded_) {
      decode();
      v_ptr_ += v_size_;
      e_ptr_ += e_size_;
      data_valid_ = false;
    } else {
      ++nbr_;
    }
    return *this;
  }

  inline Nbr operator++(int) const {
    Nbr ret(*this);
    ++(*this);
    return ret;
  }

  inline const Nbr& operator--() const {
    if (encoded_) {
      // TBD
      // v_ptr_ -= (get_varint_pre_size(*v_ptr_) + 1);
      // e_ptr_ -= (get_varint_pre_size(*e_ptr_) + 1);
      // data_valid_ = false;
    } else {
      --nbr_;
    }
    return *this;
  }

  inline Nbr operator--(int) const {
    Nbr ret(*this);
    --(*this);
    return ret;
  }

  inline bool operator==(const Nbr& rhs) const {
    if (encoded_) {
      return v_ptr_ == rhs.v_ptr_ && e_ptr_ == rhs.e_ptr_;
    }
    return nbr_ == rhs.nbr_;
  }
  inline bool operator!=(const Nbr& rhs) const {
    if (encoded_) {
      return v_ptr_ != rhs.v_ptr_ || e_ptr_ != rhs.e_ptr_;
    }
    return nbr_ != rhs.nbr_;
  }

  inline bool operator<(const Nbr& rhs) const {
    if (encoded_) {
      return v_ptr_ == rhs.v_ptr_ ? e_ptr_ < rhs.e_ptr_ : v_ptr_ < rhs.v_ptr_;
    }
    return nbr_ < rhs.nbr_;
  }

  inline const Nbr& operator*() const { return *this; }

 private:
  const mutable NbrUnit<VID_T, EID_T>* nbr_;

  const mutable uint8_t* v_ptr_;
  const mutable uint8_t* e_ptr_;
  bool encoded_;
  mutable NbrUnit<VID_T, EID_T> data_;
  mutable bool data_valid_ = false;
  mutable size_t v_size_;
  mutable size_t e_size_;

  const void** edata_arrays_;
};

template <typename VID_T, typename EID_T>
struct EncodedNbr {
 private:
  using vid_t = VID_T;
  using eid_t = EID_T;
  using prop_id_t = property_graph_types::PROP_ID_TYPE;

  EncodedNbr() {}

 public:
  EncodedNbr(const EncodedNbr& rhs)
      : ptr_(rhs.ptr_),
        data_(rhs.data_),
        size_(rhs.size()),
        edata_arrays_(rhs.edata_arrays_) {}
  EncodedNbr(EncodedNbr&& rhs)
      : ptr_(std::move(rhs.ptr_)),
        data_(rhs.data_),
        size_(rhs.size()),
        edata_arrays_(rhs.edata_arrays_) {}
  EncodedNbr(const uint8_t* ptr, size_t capacity, const void** edata_arrays)
      : ptr_(ptr), edata_arrays_(edata_arrays) {
    data_.eid = 0;
    data_.vid = 0;
    LOG(INFO) << "capacity: " << capacity;
    if (capacity > 0) {
      s_.resize(capacity);
      size_ = capacity;
      memcpy(s_.data(), ptr_, capacity);
      LOG(INFO) << "origin ptr:" << (uint64_t)ptr_;
      for (int i = 0; i < capacity; i++) {
        LOG(INFO) << std::hex << (int)s_[i];
      }
      decoder_ = folly::GroupVarint64Decoder(s_);
      decode();
    }
  }

  EncodedNbr& operator=(const EncodedNbr& rhs) {
    ptr_ = rhs.ptr_;
    data_ = rhs.data_;
    size_ = rhs.size_;
    edata_arrays_ = rhs.edata_arrays_;
    return *this;
  }

  EncodedNbr& operator=(EncodedNbr&& rhs) {
    ptr_ = std::move(rhs.ptr_);
    data_ = rhs.data_;
    size_ = rhs.size_;
    edata_arrays_ = std::move(rhs.edata_arrays_);
    return *this;
  }

  inline void decode() const {
    uint64_t eid, vid;
    size_t e_size, v_size;
    // v_size = varint_decode(ptr_, vid);
    // e_size = varint_decode(ptr_ + v_size, eid);
    if (!decoder_.next(&vid)) {
      ptr_ += size_;
      return;
    }
    
    decoder_.next(&eid);


    data_.vid += static_cast<vid_t>(vid);
    data_.eid = static_cast<eid_t>(eid);
    if(switch_ < 20) {
      LOG(INFO) << "size:" << size_;
      LOG(INFO) << "vid:" << data_.vid << " eid:" << data_.eid;
      switch_++;
    }
  }

  grape::Vertex<VID_T> neighbor() const {
    return grape::Vertex<VID_T>(data_.vid);
  }

  grape::Vertex<VID_T> get_neighbor() const {
    return grape::Vertex<VID_T>(data_.vid);
  }

  EID_T edge_id() const { return data_.eid; }

  template <typename T>
  T get_data(prop_id_t prop_id) const {
    return ValueGetter<T>::Value(edata_arrays_[prop_id], data_.eid);
  }

  std::string get_str(prop_id_t prop_id) const {
    return ValueGetter<std::string>::Value(edata_arrays_[prop_id], data_.eid);
  }

  double get_double(prop_id_t prop_id) const {
    return ValueGetter<double>::Value(edata_arrays_[prop_id], data_.eid);
  }

  int64_t get_int(prop_id_t prop_id) const {
    return ValueGetter<int64_t>::Value(edata_arrays_[prop_id], data_.eid);
  }

  inline const EncodedNbr& operator++() const {
    /*
     * This may cause the program to crash due to out-of-bounds access.
     * Currently, this part is controlled by iterators. The default user access
     * behavior will not be out of bounds.
     */
    // ptr_ += size_;
    // if(switch_ < 10) {
    //   LOG(INFO) << "ptr_" << (uint64_t)ptr_;
    // }
    
    decode();
    return *this;
  }

  inline EncodedNbr operator++(int) const {
    EncodedNbr ret(*this);
    ++(*this);
    return ret;
  }

  inline const EncodedNbr& operator--() const {
    // TBD
    return *this;
  }

  inline EncodedNbr operator--(int) const {
    EncodedNbr ret(*this);
    --(*this);
    return ret;
  }

  inline bool operator==(const EncodedNbr& rhs) const {
    return ptr_ == rhs.ptr_;
  }
  inline bool operator!=(const EncodedNbr& rhs) const {
    return ptr_ != rhs.ptr_;
  }

  inline bool operator<(const EncodedNbr& rhs) const { return ptr_ < rhs.ptr_; }

  inline const EncodedNbr& operator*() const { return *this; }

 private:
  const mutable uint8_t* ptr_;
  mutable std::string s_;

  mutable folly::GroupVarintDecoder<uint64_t> decoder_;
  mutable NbrUnit<VID_T, EID_T> data_;
  mutable size_t size_;
  mutable int switch_ = 0;

  const void** edata_arrays_;
};

template <typename VID_T>
using NbrDefault = Nbr<VID_T, property_graph_types::EID_TYPE>;

template <typename VID_T, typename EID_T>
struct OffsetNbr {
 private:
  using prop_id_t = property_graph_types::PROP_ID_TYPE;

 public:
  OffsetNbr() : nbr_(NULL), edata_table_(nullptr) {}

  OffsetNbr(const NbrUnit<VID_T, EID_T>* nbr,
            std::shared_ptr<arrow::Table> edata_table,
            const vineyard::IdParser<VID_T>* vid_parser, const VID_T* ivnums)
      : nbr_(nbr),
        edata_table_(std::move(edata_table)),
        vid_parser_(vid_parser),
        ivnums_(ivnums) {}

  OffsetNbr(const OffsetNbr& rhs)
      : nbr_(rhs.nbr_),
        edata_table_(rhs.edata_table_),
        vid_parser_(rhs.vid_parser_),
        ivnums_(rhs.ivnums_) {}

  OffsetNbr(OffsetNbr&& rhs)
      : nbr_(std::move(rhs.nbr_)),
        edata_table_(std::move(rhs.edata_table_)),
        vid_parser_(rhs.vid_parser_),
        ivnums_(rhs.ivnums_) {}

  OffsetNbr& operator=(const OffsetNbr& rhs) {
    nbr_ = rhs.nbr_;
    edata_table_ = rhs.edata_table_;
    vid_parser_ = rhs.vid_parser_;
    ivnums_ = rhs.ivnums_;
  }

  OffsetNbr& operator=(OffsetNbr&& rhs) {
    nbr_ = std::move(rhs.nbr_);
    edata_table_ = std::move(rhs.edata_table_);
    vid_parser_ = rhs.vid_parser_;
    ivnums_ = rhs.ivnums_;
  }

  grape::Vertex<VID_T> neighbor() const {
    auto offset_mask = vid_parser_->offset_mask();
    auto offset = nbr_->vid & offset_mask;
    auto v_label = vid_parser_->GetLabelId(nbr_->vid);
    auto ivnum = ivnums_[v_label];
    auto vid =
        offset < (VID_T) ivnum
            ? nbr_->vid
            : ((nbr_->vid & ~offset_mask) | (ivnum + offset_mask - offset));

    return grape::Vertex<VID_T>(vid);
  }

  EID_T edge_id() const { return nbr_->eid; }

  template <typename T>
  T get_data(prop_id_t prop_id) const {
    // the finalized vtables are guaranteed to have been concatenate
    return std::dynamic_pointer_cast<ArrowArrayType<T>>(
               edata_table_->column(prop_id)->chunk(0))
        ->Value(nbr_->eid);
  }

  inline const OffsetNbr& operator++() const {
    ++nbr_;
    return *this;
  }

  inline OffsetNbr operator++(int) const {
    OffsetNbr ret(*this);
    ++ret;
    return ret;
  }

  inline const OffsetNbr& operator--() const {
    --nbr_;
    return *this;
  }

  inline OffsetNbr operator--(int) const {
    OffsetNbr ret(*this);
    --ret;
    return ret;
  }

  inline bool operator==(const OffsetNbr& rhs) const {
    return nbr_ == rhs.nbr_;
  }
  inline bool operator!=(const OffsetNbr& rhs) const {
    return nbr_ != rhs.nbr_;
  }

  inline bool operator<(const OffsetNbr& rhs) const { return nbr_ < rhs.nbr_; }

  inline const OffsetNbr& operator*() const { return *this; }

 private:
  const mutable NbrUnit<VID_T, EID_T>* nbr_;
  std::shared_ptr<arrow::Table> edata_table_;
  const vineyard::IdParser<VID_T>* vid_parser_;
  const VID_T* ivnums_;
};

template <typename VID_T, typename EID_T>
class RawAdjList {
 public:
  RawAdjList() : begin_(NULL), end_(NULL) {}
  RawAdjList(const NbrUnit<VID_T, EID_T>* begin,
             const NbrUnit<VID_T, EID_T>* end)
      : begin_(begin), end_(end) {}

  inline const NbrUnit<VID_T, EID_T>* begin() const { return begin_; }

  inline const NbrUnit<VID_T, EID_T>* end() const { return end_; }

  inline size_t Size() const { return end_ - begin_; }

  inline bool Empty() const { return end_ == begin_; }

  inline bool NotEmpty() const { return end_ != begin_; }

  size_t size() const { return end_ - begin_; }

 private:
  const NbrUnit<VID_T, EID_T>* begin_;
  const NbrUnit<VID_T, EID_T>* end_;
};

template <typename VID_T>
using RawAdjListDefault = RawAdjList<VID_T, property_graph_types::EID_TYPE>;

template <typename VID_T, typename EID_T>
class AdjList {
 public:
  AdjList() : begin_(NULL), end_(NULL), edata_arrays_(nullptr) {}
  AdjList(const NbrUnit<VID_T, EID_T>* begin, const NbrUnit<VID_T, EID_T>* end,
          const void** edata_arrays)
      : begin_(begin), end_(end), edata_arrays_(edata_arrays) {}
  AdjList(const uint8_t* v_ptr, const uint8_t* e_ptr,
          const size_t v_begin_offset, const size_t v_end_offset,
          const size_t e_begin_offset, const size_t e_end_offset,
          const void** edata_arrays) {
    // v_begin_ptr_ = get_pointer(v_ptr, begin_index);
    // v_end_ptr_ = get_pointer(v_ptr, end_index);
    // e_begin_ptr_ = get_pointer(e_ptr, begin_index);
    // e_end_ptr_ = get_pointer(e_ptr, end_index);
    v_begin_ptr_ = v_ptr + v_begin_offset;
    v_end_ptr_ = v_ptr + v_end_offset;
    e_begin_ptr_ = e_ptr + e_begin_offset;
    e_end_ptr_ = e_ptr + e_end_offset;
    encoded_ = true;
    v_size_ = v_end_offset - v_begin_offset;
    e_size_ = e_end_offset - e_begin_offset;
  }

  inline Nbr<VID_T, EID_T> begin() const {
    if (encoded_) {
      return Nbr<VID_T, EID_T>(v_begin_ptr_, e_begin_ptr_, edata_arrays_);
    }
    return Nbr<VID_T, EID_T>(begin_, edata_arrays_);
  }

  inline Nbr<VID_T, EID_T> end() const {
    if (encoded_) {
      return Nbr<VID_T, EID_T>(v_end_ptr_, e_end_ptr_, edata_arrays_);
    }
    return Nbr<VID_T, EID_T>(end_, edata_arrays_);
  }

  inline size_t Size() const {
    if (encoded_) {
      return v_size_;
    }
    return end_ - begin_;
  }

  inline bool Empty() const {
    if (encoded_) {
      return v_size_ == 0;
    }
    return end_ == begin_;
  }

  inline bool NotEmpty() const {
    if (encoded_) {
      return v_size_ != 0;
    }

    return end_ != begin_;
  }

  // may be there are bugs.
  size_t size() const {
    if (encoded_) {
      return v_size_;
    }
    return end_ - begin_;
  }

  inline const NbrUnit<VID_T, EID_T>* begin_unit() const { return begin_; }

  inline const NbrUnit<VID_T, EID_T>* end_unit() const { return end_; }

 private:
  const NbrUnit<VID_T, EID_T>* begin_;
  const NbrUnit<VID_T, EID_T>* end_;

  const uint8_t* v_begin_ptr_;
  const uint8_t* v_end_ptr_;
  const uint8_t* e_begin_ptr_;
  const uint8_t* e_end_ptr_;
  size_t e_size_ = 0;
  size_t v_size_ = 0;
  bool encoded_ = false;

  const void** edata_arrays_;
};

template <typename VID_T, typename EID_T>
class EncodedAdjList {
 public:
  EncodedAdjList()
      : begin_ptr_(nullptr),
        end_ptr_(nullptr),
        size_(0),
        edata_arrays_(nullptr) {}
  EncodedAdjList(const uint8_t* ptr, const size_t begin_offset,
                 const size_t end_offset, const void** edata_arrays) {
    begin_ptr_ = ptr + begin_offset;
    end_ptr_ = ptr + end_offset;
    size_ = end_offset - begin_offset;
  }

  inline EncodedNbr<VID_T, EID_T> begin() const {
    return EncodedNbr<VID_T, EID_T>(begin_ptr_, size_, edata_arrays_);
  }

  inline EncodedNbr<VID_T, EID_T> end() const {
    return EncodedNbr<VID_T, EID_T>(end_ptr_, 0, edata_arrays_);
  }

  inline size_t Size() const { return size_; }

  inline bool Empty() const { return size_ == 0; }

  inline bool NotEmpty() const { return size_ != 0; }

  // may be there are bugs.
  size_t size() const { return size_; }

  // inline const NbrUnit<VID_T, EID_T>* begin_unit() const { return begin_; }

  // inline const NbrUnit<VID_T, EID_T>* end_unit() const { return end_; }

 private:
  const uint8_t* begin_ptr_;
  const uint8_t* end_ptr_;
  size_t size_ = 0;

  const void** edata_arrays_;
};

template <typename VID_T>
using AdjListDefault = AdjList<VID_T, property_graph_types::EID_TYPE>;

/**
 * OffsetAdjList will offset the outer vertices' lid, makes it between "ivnum"
 * and "tvnum" instead of "ivnum ~ tvnum - outer vertex index"
 *
 * @tparam VID_T
 * @tparam EID_T
 */
template <typename VID_T, typename EID_T>
class OffsetAdjList {
 public:
  OffsetAdjList()
      : begin_(NULL),
        end_(NULL),
        edata_table_(nullptr),
        vid_parser_(nullptr),
        ivnums_(nullptr) {}

  OffsetAdjList(const NbrUnit<VID_T, EID_T>* begin,
                const NbrUnit<VID_T, EID_T>* end,
                std::shared_ptr<arrow::Table> edata_table,
                const vineyard::IdParser<VID_T>* vid_parser,
                const VID_T* ivnums)
      : begin_(begin),
        end_(end),
        edata_table_(std::move(edata_table)),
        vid_parser_(vid_parser),
        ivnums_(ivnums) {}

  inline OffsetNbr<VID_T, EID_T> begin() const {
    return OffsetNbr<VID_T, EID_T>(begin_, edata_table_, vid_parser_, ivnums_);
  }

  inline OffsetNbr<VID_T, EID_T> end() const {
    return OffsetNbr<VID_T, EID_T>(end_, edata_table_, vid_parser_, ivnums_);
  }

  inline size_t Size() const { return end_ - begin_; }

  inline bool Empty() const { return end_ == begin_; }

  inline bool NotEmpty() const { return end_ != begin_; }

  size_t size() const { return end_ - begin_; }

 private:
  const NbrUnit<VID_T, EID_T>* begin_;
  const NbrUnit<VID_T, EID_T>* end_;
  std::shared_ptr<arrow::Table> edata_table_;
  const vineyard::IdParser<VID_T>* vid_parser_;
  const VID_T* ivnums_;
};

}  // namespace property_graph_utils

inline std::string generate_type_name(
    const std::string& template_name,
    const std::vector<std::string>& template_params) {
  if (template_params.empty()) {
    return template_name + "<>";
  }
  std::string ret = template_name;
  ret += ("<" + template_params[0]);
  for (size_t i = 1; i < template_params.size(); ++i) {
    ret += ("," + template_params[i]);
  }
  ret += ">";
  return ret;
}

class EmptyArray {
  using value_type = grape::EmptyType;

 public:
  explicit EmptyArray(int64_t size) : size_(size) {}

  value_type Value(int64_t offset) { return value_type(); }
  int64_t length() const { return size_; }

  const value_type* raw_values() { return NULL; }

 private:
  int64_t size_;
};

template <typename DATA_T>
typename std::enable_if<std::is_same<DATA_T, grape::EmptyType>::value,
                        std::shared_ptr<ArrowArrayType<DATA_T>>>::type
assign_array(std::shared_ptr<arrow::Array>, int64_t length) {
  return std::make_shared<EmptyArray>(length);
}

template <typename DATA_T>
typename std::enable_if<!std::is_same<DATA_T, grape::EmptyType>::value,
                        std::shared_ptr<ArrowArrayType<DATA_T>>>::type
assign_array(std::shared_ptr<arrow::Array> array, int64_t) {
  return std::dynamic_pointer_cast<ArrowArrayType<DATA_T>>(array);
}

template <>
struct ConvertToArrowType<::grape::EmptyType> {
  using ArrayType = EmptyArray;
  static std::shared_ptr<arrow::DataType> TypeValue() { return arrow::null(); }
};

}  // namespace vineyard

#endif  // MODULES_GRAPH_FRAGMENT_PROPERTY_GRAPH_TYPES_H_
