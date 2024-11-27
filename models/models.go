package models

type AlertConfig struct {
	MobileNo       string        `bson:"mobileNo" json:"mobileNo"`
	IsAlertDisable bool          `bson:"isAlertDisable" json:"isAlertDisable"`
	AlarmConfig    []AlarmConfig `bson:"alarmConfig" json:"alarmConfig"`
	Imei           string        `bson:"imei" json:"imei"`
}

type AlarmConfig struct {
	Event                  string   `bson:"event" json:"event"`
	Location               Location `bson:"location" json:"location"`
	IsAlreadyGenerateAlert bool     `bson:"isAlreadyGenerateAlert" json:"isAlreadyGenerateAlert"`
	StartDate              string   `bson:"startDate" json:"startDate"`
	EndDate                string   `bson:"endDate" json:"endDate"`
	StartAlertTime         string   `bson:"startAlertTime" json:"startAlertTime"`
	EndAlertTime           string   `bson:"endAlertTime" json:"endAlertTime"`
	ThresholdValue         float64  `bson:"thresholdValue" json:"thresholdValue"` // For overspeed and battery alerts
	Duration               string   `bson:"duration" json:"duration"`             // For overstop alert
}

type Location struct {
	ID                     string      `bson:"_id" json:"_id"`
	Name                   string      `bson:"name" json:"name"`
	LocationType           string      `bson:"locationType" json:"locationType"`
	GeoCodeData            GeoCodeData `bson:"geoCodeData" json:"geoCodeData"`
	IsAlreadyGenerateAlert bool        `bson:"isAlreadyGenerateAlert" json:"isAlreadyGenerateAlert"`
}

type GeoCodeData struct {
	Type     string   `bson:"type" json:"type"`
	Geometry Geometry `bson:"geometry" json:"geometry"`
}

type Geometry struct {
	Type        string    `bson:"type" json:"type"`
	Coordinates []float64 `bson:"coordinates" json:"coordinates"`
	Radius      float64   `bson:"radius" json:"radius"`
}
