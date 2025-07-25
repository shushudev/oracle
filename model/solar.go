package model

type SolarData struct {
	DeviceID   string  `json:"device_id"`
	Timestamp  string  `json:"timestamp"`
	Irradiance float64 `json:"irradiance"`
	Latitude   float64 `json:"latitude"`
	Longitude  float64 `json:"longitude"`
}
