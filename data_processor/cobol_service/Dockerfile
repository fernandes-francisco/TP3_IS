FROM ubuntu:22.04
RUN apt-get update && apt-get install -y gnucobol socat
WORKDIR /app
COPY sma_calc.cbl .
RUN cobc -x -free -o sma_calc sma_calc.cbl
EXPOSE 4000
CMD socat TCP-LISTEN:4000,reuseaddr,fork EXEC:"./sma_calc",pty,stderr
