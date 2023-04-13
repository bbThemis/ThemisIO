FROM tacc/tacc-centos7-impi19.0.7-common:latest

ADD . / /ThemisIO-build
RUN sed -i 's/compilers_and_libraries_2018.6.288/compilers_and_libraries/g' /ThemisIO-build/Makefile 
RUN cd /ThemisIO-build \
    && cd src/client \
    && ./compile.sh 

FROM tacc/tacc-centos7-impi19.0.7-common:latest
RUN mkdir /ThemisIO-client
COPY --from=0 /ThemisIO-build/wrapper.so /ThemisIO-client/
#ENV LD_PRELOAD=/ThemisIO-client/wrapper.so
