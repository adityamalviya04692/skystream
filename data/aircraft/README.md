Drop the OpenSky aircraft database CSV here (https://opensky-network.org/data/aircraft).
The `aircraft-loader` service picks up any `*.csv`, publishes Indian-registered (VT-) aircraft to the
`ref.aircraft` Kafka topic, and moves the file to `processed/`.
