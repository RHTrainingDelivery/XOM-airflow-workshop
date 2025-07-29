INSERT INTO weather ( 
    Id,
    MinTemp,
    MaxTemp,
    Rainfall,
    Evaporation,
    Sunshine,
    WindGustDir,
    WindGustSpeed ,
    WindDir9am,
    WindDir3pm,
    WindSpeed3pm ,
    WindSpeed9am,
    Humidity9am,
    Humidity3pm,
    Pressure9am,
    Pressure3pm,
    Cloud9am,
    Cloud3pm,
    Temp9am,
    Temp3pm,
    RainToday,
    RISK_MM,  
    RainTomorrow 
    )
SELECT     
    Id,
    MinTemp,
    MaxTemp,
    Rainfall,
    Evaporation,
    Sunshine,
    WindGustDir,
    CASE
        WHEN WindGustSpeed='NA' THEN NULL,
        ELSE WindGustSpeed
    END,
    WindDir9am,
    WindDir3pm,
    CASE
        WHEN WindSpeed3pm='NA' THEN NULL,
        ELSE WindSpeed3pm
    END,
    CASE
        WHEN WindSpeed9am='NA' THEN NULL,
        ELSE WindSpeed9am
    END,
    Humidity9am,
    Humidity3pm,
    Pressure9am,
    Pressure3pm,
    Cloud9am,
    Cloud3pm,
    Temp9am,
    Temp3pm,
    RainToday,
    RISK_MM,  
    RainTomorrow
FROM  weather_temp; 