GO_CIRCULAR_Q_FOLDER = circularq/*
SEED_FOLDER = seed/*
CONFIG_FOLDER = ../config 

test: $(GO_CIRCULAR_Q_FOLDER)
	CONFIG_FOLDER=$(CONFIG_FOLDER) go test ./circularq -v
