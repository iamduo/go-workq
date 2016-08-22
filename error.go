package workq

type ResponseError struct {
	code string
	text string
}

func NewResponseError(code string, text string) error {
	return &ResponseError{code: code, text: text}
}

func (e *ResponseError) Error() string {
	if e.text != "" {
		return e.code + " " + e.text
	}

	return e.code
}

func (e *ResponseError) Code() string {
	return e.code
}

func (e *ResponseError) Text() string {
	return e.text
}

type NetError struct {
	text string
}

func (e *NetError) Error() string {
	return "Net Error: " + e.text
}

func NewNetError(text string) error {
	return &NetError{text: text}
}
