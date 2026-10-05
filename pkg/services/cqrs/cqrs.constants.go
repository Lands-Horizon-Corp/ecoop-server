package cqrs

const (
	ModeEqual       Mode = "equal"
	ModeNotEqual    Mode = "notEqual"
	ModeContains    Mode = "contains"
	ModeNotContains Mode = "notContains"
	ModeStartsWith  Mode = "startsWith"
	ModeEndsWith    Mode = "endsWith"
	ModeSearch      Mode = "search"
	ModeCustom      Mode = "custom"
	ModeInside      Mode = "inside"
	ModeOutside     Mode = "outside"
	ModeGT          Mode = "gt"
	ModeGTE         Mode = "gte"
	ModeLT          Mode = "lt"
	ModeLTE         Mode = "lte"
	ModeRange       Mode = "range"
	ModeBefore      Mode = "before"
	ModeAfter       Mode = "after"
	ModeIsEmpty     Mode = "isEmpty"
	ModeIsNotEmpty  Mode = "isNotEmpty"

	DataTypeNumber DataType = "number"
	DataTypeText   DataType = "text"
	DataTypeBool   DataType = "bool"
	DataTypeDate   DataType = "date"
	DataTypeTime   DataType = "time"

	LogicAnd Logic = "and"
	LogicOr  Logic = "or"

	SortOrderAsc  SortOrder = "asc"
	SortOrderDesc SortOrder = "desc"

	ChangeTypeCreated ChangeType = iota
	ChangeTypeUpdated
	ChangeTypeDeleted
)
