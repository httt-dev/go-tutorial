// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.34.2
// 	protoc        v5.27.1
// source: screen_message.proto

package pb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Screen_Panel int32

const (
	Screen_UNKNOWN Screen_Panel = 0
	Screen_IPS     Screen_Panel = 1
	Screen_OLED    Screen_Panel = 2
)

// Enum value maps for Screen_Panel.
var (
	Screen_Panel_name = map[int32]string{
		0: "UNKNOWN",
		1: "IPS",
		2: "OLED",
	}
	Screen_Panel_value = map[string]int32{
		"UNKNOWN": 0,
		"IPS":     1,
		"OLED":    2,
	}
)

func (x Screen_Panel) Enum() *Screen_Panel {
	p := new(Screen_Panel)
	*p = x
	return p
}

func (x Screen_Panel) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Screen_Panel) Descriptor() protoreflect.EnumDescriptor {
	return file_screen_message_proto_enumTypes[0].Descriptor()
}

func (Screen_Panel) Type() protoreflect.EnumType {
	return &file_screen_message_proto_enumTypes[0]
}

func (x Screen_Panel) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Screen_Panel.Descriptor instead.
func (Screen_Panel) EnumDescriptor() ([]byte, []int) {
	return file_screen_message_proto_rawDescGZIP(), []int{0, 0}
}

type Screen struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	SizeInch   float32            `protobuf:"fixed32,1,opt,name=size_inch,json=sizeInch,proto3" json:"size_inch,omitempty"`
	Resolution *Screen_Resolution `protobuf:"bytes,2,opt,name=resolution,proto3" json:"resolution,omitempty"`
	Panel      Screen_Panel       `protobuf:"varint,3,opt,name=panel,proto3,enum=httt.dev.pcbook.Screen_Panel" json:"panel,omitempty"`
	Multitouch bool               `protobuf:"varint,4,opt,name=multitouch,proto3" json:"multitouch,omitempty"`
}

func (x *Screen) Reset() {
	*x = Screen{}
	if protoimpl.UnsafeEnabled {
		mi := &file_screen_message_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Screen) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Screen) ProtoMessage() {}

func (x *Screen) ProtoReflect() protoreflect.Message {
	mi := &file_screen_message_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Screen.ProtoReflect.Descriptor instead.
func (*Screen) Descriptor() ([]byte, []int) {
	return file_screen_message_proto_rawDescGZIP(), []int{0}
}

func (x *Screen) GetSizeInch() float32 {
	if x != nil {
		return x.SizeInch
	}
	return 0
}

func (x *Screen) GetResolution() *Screen_Resolution {
	if x != nil {
		return x.Resolution
	}
	return nil
}

func (x *Screen) GetPanel() Screen_Panel {
	if x != nil {
		return x.Panel
	}
	return Screen_UNKNOWN
}

func (x *Screen) GetMultitouch() bool {
	if x != nil {
		return x.Multitouch
	}
	return false
}

type Screen_Resolution struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Width  uint32 `protobuf:"varint,1,opt,name=width,proto3" json:"width,omitempty"`
	Height uint32 `protobuf:"varint,2,opt,name=height,proto3" json:"height,omitempty"`
}

func (x *Screen_Resolution) Reset() {
	*x = Screen_Resolution{}
	if protoimpl.UnsafeEnabled {
		mi := &file_screen_message_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Screen_Resolution) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Screen_Resolution) ProtoMessage() {}

func (x *Screen_Resolution) ProtoReflect() protoreflect.Message {
	mi := &file_screen_message_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Screen_Resolution.ProtoReflect.Descriptor instead.
func (*Screen_Resolution) Descriptor() ([]byte, []int) {
	return file_screen_message_proto_rawDescGZIP(), []int{0, 0}
}

func (x *Screen_Resolution) GetWidth() uint32 {
	if x != nil {
		return x.Width
	}
	return 0
}

func (x *Screen_Resolution) GetHeight() uint32 {
	if x != nil {
		return x.Height
	}
	return 0
}

var File_screen_message_proto protoreflect.FileDescriptor

var file_screen_message_proto_rawDesc = []byte{
	0x0a, 0x14, 0x73, 0x63, 0x72, 0x65, 0x65, 0x6e, 0x5f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0f, 0x68, 0x74, 0x74, 0x74, 0x2e, 0x64, 0x65, 0x76,
	0x2e, 0x70, 0x63, 0x62, 0x6f, 0x6f, 0x6b, 0x22, 0xa3, 0x02, 0x0a, 0x06, 0x53, 0x63, 0x72, 0x65,
	0x65, 0x6e, 0x12, 0x1b, 0x0a, 0x09, 0x73, 0x69, 0x7a, 0x65, 0x5f, 0x69, 0x6e, 0x63, 0x68, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x02, 0x52, 0x08, 0x73, 0x69, 0x7a, 0x65, 0x49, 0x6e, 0x63, 0x68, 0x12,
	0x42, 0x0a, 0x0a, 0x72, 0x65, 0x73, 0x6f, 0x6c, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x22, 0x2e, 0x68, 0x74, 0x74, 0x74, 0x2e, 0x64, 0x65, 0x76, 0x2e, 0x70,
	0x63, 0x62, 0x6f, 0x6f, 0x6b, 0x2e, 0x53, 0x63, 0x72, 0x65, 0x65, 0x6e, 0x2e, 0x52, 0x65, 0x73,
	0x6f, 0x6c, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x0a, 0x72, 0x65, 0x73, 0x6f, 0x6c, 0x75, 0x74,
	0x69, 0x6f, 0x6e, 0x12, 0x33, 0x0a, 0x05, 0x70, 0x61, 0x6e, 0x65, 0x6c, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x0e, 0x32, 0x1d, 0x2e, 0x68, 0x74, 0x74, 0x74, 0x2e, 0x64, 0x65, 0x76, 0x2e, 0x70, 0x63,
	0x62, 0x6f, 0x6f, 0x6b, 0x2e, 0x53, 0x63, 0x72, 0x65, 0x65, 0x6e, 0x2e, 0x50, 0x61, 0x6e, 0x65,
	0x6c, 0x52, 0x05, 0x70, 0x61, 0x6e, 0x65, 0x6c, 0x12, 0x1e, 0x0a, 0x0a, 0x6d, 0x75, 0x6c, 0x74,
	0x69, 0x74, 0x6f, 0x75, 0x63, 0x68, 0x18, 0x04, 0x20, 0x01, 0x28, 0x08, 0x52, 0x0a, 0x6d, 0x75,
	0x6c, 0x74, 0x69, 0x74, 0x6f, 0x75, 0x63, 0x68, 0x1a, 0x3a, 0x0a, 0x0a, 0x52, 0x65, 0x73, 0x6f,
	0x6c, 0x75, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x14, 0x0a, 0x05, 0x77, 0x69, 0x64, 0x74, 0x68, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x05, 0x77, 0x69, 0x64, 0x74, 0x68, 0x12, 0x16, 0x0a, 0x06,
	0x68, 0x65, 0x69, 0x67, 0x68, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x06, 0x68, 0x65,
	0x69, 0x67, 0x68, 0x74, 0x22, 0x27, 0x0a, 0x05, 0x50, 0x61, 0x6e, 0x65, 0x6c, 0x12, 0x0b, 0x0a,
	0x07, 0x55, 0x4e, 0x4b, 0x4e, 0x4f, 0x57, 0x4e, 0x10, 0x00, 0x12, 0x07, 0x0a, 0x03, 0x49, 0x50,
	0x53, 0x10, 0x01, 0x12, 0x08, 0x0a, 0x04, 0x4f, 0x4c, 0x45, 0x44, 0x10, 0x02, 0x42, 0x14, 0x5a,
	0x12, 0x68, 0x74, 0x74, 0x74, 0x2e, 0x64, 0x65, 0x76, 0x2f, 0x70, 0x63, 0x62, 0x6f, 0x6f, 0x6b,
	0x2f, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_screen_message_proto_rawDescOnce sync.Once
	file_screen_message_proto_rawDescData = file_screen_message_proto_rawDesc
)

func file_screen_message_proto_rawDescGZIP() []byte {
	file_screen_message_proto_rawDescOnce.Do(func() {
		file_screen_message_proto_rawDescData = protoimpl.X.CompressGZIP(file_screen_message_proto_rawDescData)
	})
	return file_screen_message_proto_rawDescData
}

var file_screen_message_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_screen_message_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_screen_message_proto_goTypes = []any{
	(Screen_Panel)(0),         // 0: httt.dev.pcbook.Screen.Panel
	(*Screen)(nil),            // 1: httt.dev.pcbook.Screen
	(*Screen_Resolution)(nil), // 2: httt.dev.pcbook.Screen.Resolution
}
var file_screen_message_proto_depIdxs = []int32{
	2, // 0: httt.dev.pcbook.Screen.resolution:type_name -> httt.dev.pcbook.Screen.Resolution
	0, // 1: httt.dev.pcbook.Screen.panel:type_name -> httt.dev.pcbook.Screen.Panel
	2, // [2:2] is the sub-list for method output_type
	2, // [2:2] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_screen_message_proto_init() }
func file_screen_message_proto_init() {
	if File_screen_message_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_screen_message_proto_msgTypes[0].Exporter = func(v any, i int) any {
			switch v := v.(*Screen); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_screen_message_proto_msgTypes[1].Exporter = func(v any, i int) any {
			switch v := v.(*Screen_Resolution); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_screen_message_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_screen_message_proto_goTypes,
		DependencyIndexes: file_screen_message_proto_depIdxs,
		EnumInfos:         file_screen_message_proto_enumTypes,
		MessageInfos:      file_screen_message_proto_msgTypes,
	}.Build()
	File_screen_message_proto = out.File
	file_screen_message_proto_rawDesc = nil
	file_screen_message_proto_goTypes = nil
	file_screen_message_proto_depIdxs = nil
}
