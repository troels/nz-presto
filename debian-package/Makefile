# Presto Server

# IMPORTANT: Update the following line when you want to build a new version:
version = 0.179-t.0.1-nz.3

SHELL := /bin/bash
name = presto-server
url = http://gitlab.nzcorp.net/daa/nz-presto
full_name = $(name)-$(version)
full_path = $(shell pwd)/../presto-server/target/$(full_name).tar.gz

.PHONY: changelog

all: package

dist-clean: clean
	pushd ..; \
	./mvnw -DskipTests clean; \
	popd

clean:
	rm -rf build; \
	rm -rf tmp; \
	mkdir -p tmp; \
	mkdir -p build

$(full_path):
	pushd ..; \
	./mvnw -DskipTests clean install; \
	popd

$(name): $(full_path)
	cd tmp; \
	echo $(full_path); \
	tar zxvf $(full_path); \
	mv $(full_name) $(name)

prep-build: $(name)
	mkdir -p build/usr/share/presto; \
	mkdir -p build/etc/init.d; \
	mkdir -p build/etc/presto; \
	mkdir -p build/var/lib/presto; \
	mkdir -p build/var/log; \
	pushd build/usr/share/presto; \
	ln -sf /var/lib/presto data; \
	ln -sf /etc/presto etc; \
	ln -sf /var/lib/presto/var/log log; \
	popd; \
	pushd build/var/log; \
	ln -sf /var/lib/presto/var/log presto; \
	popd;

build: prep-build
	cp -rp tmp/$(name)/* build/usr/share/presto; \
	cp -rp scripts/$(name) build/etc/init.d/; \
	cp -rp configs/* build/etc/presto/;

changelog:
	grep -q "$(name) ($(version))" changelog || /usr/bin/dch -v "$(version)" "Prepare for release" -D unstable -c changelog

package: clean changelog build
	pushd build; \
		fpm -a all -s dir -t deb -d "uuid-runtime" -n $(name) -v $(version) --deb-changelog ../changelog --url $(url) --after-install ../scripts/postinst -- .; \
	mkdir -p ../releases; \
	mv *.deb ../releases/; \
	popd;

docker: package
	docker build . --tag sal:5000/daa/presto-origin:latest
	docker push sal:5000/daa/presto-origin:latest
