# Pipeline automation configuration paths
PROTO_DIR := proto
GEN_DIR := gen/go
PROTO_SRC := $(PROTO_DIR)/vehicle/v1/telemetry.proto

.PHONY: all compile clean directories

# Default target executes complete lifecycle
all: clean directories compile

# Create the compilation target directory tree structure securely
directories:
	@mkdir -p $(GEN_DIR)

# Invoke the protocol buffers compiler with strict Go module configuration constraints
compile:
	@protoc --proto_path=proto --go_out=gen/go --go_opt=paths=source_relative proto/vehicle/v1/telemetry.proto
	@echo "✅ Telemetry Protobuf schemas successfully compiled into $(GEN_DIR)"

# Clean existing generated code fragments to ensure clean builds
clean:
	@rm -rf $(GEN_DIR)
	@echo "🧹 Cleaned old generated schema fragments."
