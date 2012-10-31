package drivenow

type Vehicle struct {
	Name         string   `json:"carName"`
	Fuel         int      `json:"fuelState,string"`
	Cleanliness  string   `json:"innerCleanliness"`
	LicensePlate string   `json:"licensePlate"`
	Position     Position `json:"position"`
}

type Position struct {
	Address   string  `json:"address"`
	Latitude  float64 `json:"latitude,string"`
	Longitude float64 `json:"longitude,string"`
}
