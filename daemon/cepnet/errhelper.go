package cepnet

func createErrMessage(err error) *ErrorMessage {
	return &ErrorMessage{
		Code:    0,
		Message: err.Error(),
	}
}
