APP_NAME:=gorm-to-dm
APP_PATH:=$(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
SCRIPT_PATH:=$(APP_PATH)/scripts
COMPILE_OUT:=$(APP_PATH)/bin/$(APP_NAME)
PROTO:=protos

build:
	@echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>making build app<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
	@chmod +x $(SCRIPT_PATH)/build/*.sh
	@cd $(APP_PATH) && $(SCRIPT_PATH)/build/gobuild.sh $(APP_NAME) $(COMPILE_OUT)
	@echo -e "\n"

docker-test:
	@docker-compose -f docker-compose.yml up --build --abort-on-container-exit
docker-remove:
	@docker-compose -f docker-compose.yml rm -v
docker-build:
	@docker-compose -f docker-compose.yml up

