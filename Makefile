
# Enable debug output
debug ?= 0

# DockerHub image to build FROM: openjdk:$(open_jdk_version)
open_jdk_version ?= jdk8u265-b01-centos
open_jdk_image ?= adoptopenjdk/openjdk8

#Version of sbt package to install: https://dl.bintray.com/sbt/debian/sbt-$(sbt_version).deb
# Must match entry in project/build.properties
sbt_version ?= 1.3.3

mesos_version ?= 1.9.0

# Docker image tag
# This must exist as ${docker_image_name}:$(docker_image_tag) in either:
## Local docker image cache 
## DockerHub 
docker_image_tag=$(open_jdk_version)-$(sbt_version)
docker_image_name=mesos/sbt
docker_builder=builder

dockerfile_url=https://raw.githubusercontent.com/mozilla/docker-sbt/main/Dockerfile

quiet := @
sbt_debug_flag :=
ifneq ($(debug),0)
ifneq ($(debug),)
$(info Debug output enabled at level $(debug))
quiet =
sbt_debug_flag := -v
endif
endif

# Create a default target
.PHONY: no_targets__
@no_targets__: list

# List the make targets
.PHONY: list
list: 
	$(quiet) echo "List make targets:" &&\
	cat Makefile | grep "^[A-Za-z0-9_-]\+:" | grep -v "^_[A-Za-z0-9_-]\+" | awk '{print $$1}' | sed "s/://g" | sed "s/^/   /g" | sort

# Create the docker entrypoint file
.PHONY: _create-dockerfile
_create-dockerfile:
	$(quiet) \
	mkdir -p $(docker_builder) &&\
	echo 'ARG OPENJDK_TAG=$(open_jdk_version)'                                                              > $(docker_builder)/Dockerfile &&\
	echo 'ARG OPENJDK_NAME=$(open_jdk_image)'                                                              >> $(docker_builder)/Dockerfile &&\
	echo 'FROM $${OPENJDK_NAME}:$${OPENJDK_TAG}'                                                           >> $(docker_builder)/Dockerfile &&\
	echo 'ARG SBT_VERSION=$(sbt_version)'                                                                  >> $(docker_builder)/Dockerfile &&\
	echo 'ARG MESOS_VERSION=$(mesos_version)'                                                              >> $(docker_builder)/Dockerfile &&\
	echo 'ENV DOCKER_FROM=$${OPENJDK_NAME}:$${OPENJDK_TAG}'                                                >> $(docker_builder)/Dockerfile &&\
	echo 'ENV SBT_VERSION=$${SBT_VERSION}'                                                                 >> $(docker_builder)/Dockerfile &&\
	echo 'ENV MESOS_VERSION=$${MESOS_VERSION}'                                                             >> $(docker_builder)/Dockerfile &&\
	echo 'RUN \'                                                                                           >> $(docker_builder)/Dockerfile &&\
	echo '	curl -L -o /etc/yum.repos.d/bintray-sbt-rpm.repo https://bintray.com/sbt/rpm/rpm &&\'          >> $(docker_builder)/Dockerfile &&\
	echo '	rpm -Uvh http://repos.mesosphere.io/el/7/noarch/RPMS/mesosphere-el-repo-7-1.noarch.rpm &&\'    >> $(docker_builder)/Dockerfile &&\
	echo '	yum install -y sbt-$${SBT_VERSION} mesos-$${MESOS_VERSION} git'                                >> $(docker_builder)/Dockerfile


# Build local copy of docker image if public image doesn't exist
.PHONY: docker-build
docker-build: _create-dockerfile
	$(quiet) \
	mkdir -p $(docker_builder) &&\
	docker build \
		--rm \
		--tag $(docker_image_name):$(docker_image_tag)  \
		-f $(docker_builder)/Dockerfile \
		.	


# Create the docker entrypoint file
.PHONY: _create-entrypoint
_create-entrypoint:
	$(quiet) \
	mkdir -p $(docker_builder) &&\
	echo "#!/bin/bash -eu"                                                               > $(docker_builder)/entrypoint.sh &&\
	echo 'USER_NAME=$(docker_builder)'                                                  >> $(docker_builder)/entrypoint.sh &&\
	echo 'echo "USER_UID=$${USER_UID}"'                                                 >> $(docker_builder)/entrypoint.sh &&\
	echo 'echo "USER_GID=$${USER_GID}"'                                                 >> $(docker_builder)/entrypoint.sh &&\
	echo 'if ! getent group $${USER_GID} &>/dev/null; then'                             >> $(docker_builder)/entrypoint.sh &&\
	echo '  groupadd --non-unique -g $${USER_GID} $${USER_NAME}'                        >> $(docker_builder)/entrypoint.sh &&\
	echo 'fi'                                                                           >> $(docker_builder)/entrypoint.sh &&\
	echo 'if ! getent passwd $${USER_UID} &>/dev/null; then'                            >> $(docker_builder)/entrypoint.sh &&\
	echo '  useradd --non-unique -u $${USER_UID} -g $${USER_GID} $${USER_NAME} -d /app' >> $(docker_builder)/entrypoint.sh &&\
	echo 'fi'                                                                           >> $(docker_builder)/entrypoint.sh &&\
	echo 'su $${USER_NAME} -c "sbt $$@"'                                                >> $(docker_builder)/entrypoint.sh &&\
	chmod u+x $(docker_builder)/entrypoint.sh


# Clean the target directories and sbt cache
# Also cleans files created when using docker-shell-root
.PHONY: deep-clean
deep-clean: 
	$(quiet) \
	sudo find . -type d -name target -prune -exec rm -r {} + &&\
	sudo rm -rf $(docker_builder) &&\
	echo "Cleaning complete"

.PHONY: _sbt-run
_sbt-run:
	$(quiet) \
	docker run -it --rm \
		--entrypoint "/app/$(docker_builder)/entrypoint.sh" \
		-e USER_GID=$(shell id -g) \
		-e USER_UID=$(shell id -u) \
	  -v $$PWD:/app \
	  -v $$PWD/$(docker_builder)/.sbt:/home/$(docker_builder)/.sbt \
	  -v $$PWD/$(docker_builder)/.ivy2:/home/$(docker_builder)/.ivy2 \
	  -v $$PWD/$(docker_builder)/.cache:/home/$(docker_builder)/.cache \
	  -w /app \
	  $(docker_image_name):$(docker_image_tag) \
		$(sbt_debug_flag) $(sbt_command)

.PHONY: sbt-clean
sbt-clean: _create-entrypoint
	$(quiet) \
	$(MAKE) sbt_command=clean debug=$(debug) open_jdk_version=$(open_jdk_version) sbt_version=$(sbt_version) _sbt-run

.PHONY: sbt-compile
sbt-compile: _create-entrypoint
	$(quiet) \
	$(MAKE) sbt_command=compile debug=$(debug) open_jdk_version=$(open_jdk_version) sbt_version=$(sbt_version) _sbt-run

.PHONY: sbt-test
sbt-test: _create-entrypoint
	$(quiet) \
	$(MAKE) sbt_command=test debug=$(debug) open_jdk_version=$(open_jdk_version) sbt_version=$(sbt_version) _sbt-run

.PHONY: sbt-package
sbt-package: _create-entrypoint 
	$(quiet) \
	$(MAKE) sbt_command=package debug=$(debug) open_jdk_version=$(open_jdk_version) sbt_version=$(sbt_version) _sbt-run &&\
	targetPath=$$(ls -1 target/scala*/*.jar) &&\
	echo "Plugin JAR file: $${targetPath}"

.PHONY: sbt-shell
sbt-shell: _create-entrypoint
	$(quiet) \
	$(MAKE) sbt_command=shell debug=$(debug) open_jdk_version=$(open_jdk_version) sbt_version=$(sbt_version) _sbt-run

# BEWARE: Compiling from this bash shell can create build artifacts owned by root; use deep-clean to remove
# Uses a separate set of sbt cache directories
.PHONY: docker-shell
docker-shell:
	$(quiet) \
	docker run -it --rm \
	  -v $$PWD:/app \
	  -v $$PWD/$(docker_builder)/root/.sbt:/root/.sbt \
	  -v $$PWD/$(docker_builder)/root/.ivy2:/root/.ivy2 \
	  -v $$PWD/$(docker_builder)/root/.cache:/root/.cache \
	  -w /app \
	  $(docker_image_name):$(docker_image_tag) \
		/bin/bash
    