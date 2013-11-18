package api

import "errors"

var (
    DuplicateIndex   = errors.New("Index by the specified name already exists")
    NoSuchIndex      = errors.New("Index by the specified name does not exist")
    NoSuchType       = errors.New("The specified index type is not defined")
    DDocChanged      = errors.New("The design doc has been changed externally")
    DDocCreateFailed = errors.New("Unable to create design doc for index")
    ExprNotSupported = errors.New("Expression type is not supported")
)
