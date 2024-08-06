package models

type Product struct {
	Name string `json:"name"`
	Expired  string `json:"expired"`
	Brand string `json:"brand"`
}

type Operation struct {
	Op      string  `json:"op"`
	Product Product `json:"product"`
}
