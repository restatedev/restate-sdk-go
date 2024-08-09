package errors

import (
	"fmt"

	protocol "github.com/restatedev/sdk-go/generated/dev/restate/service"
)

type Code uint16

const (
	ErrJournalMismatch   Code = 570
	ErrProtocolViolation Code = 571
)

var (
	ErrKeyNotFound = NewTerminalError(fmt.Errorf("key not found"), 404)
)

type CodeError struct {
	Code  Code
	Inner error
}

func (e *CodeError) Error() string {
	return fmt.Sprintf("[%d] %s", e.Code, e.Inner)
}

func (e *CodeError) Unwrap() error {
	return e.Inner
}

type TerminalError struct {
	Inner error
}

func (e *TerminalError) Error() string {
	return e.Inner.Error()
}

func (e *TerminalError) Unwrap() error {
	return e.Inner
}

func ErrorFromFailure(failure *protocol.Failure) error {
	return &CodeError{Inner: &TerminalError{Inner: fmt.Errorf(failure.Message)}, Code: Code(failure.Code)}
}

func NewTerminalError(err error, code ...Code) error {
	if err == nil {
		return nil
	}

	if len(code) > 1 {
		panic("only single code is allowed")
	}

	err = &TerminalError{
		Inner: err,
	}

	if len(code) == 1 {
		err = &CodeError{
			Inner: err,
			Code:  code[0],
		}
	}

	return err
}
