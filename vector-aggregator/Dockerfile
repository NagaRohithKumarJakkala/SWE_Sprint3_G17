FROM timberio/vector:0.38.0-debian
COPY vector.yaml /etc/vector/vector.yaml
COPY vrl/ /etc/vector/vrl/
EXPOSE 4317 8686 8687
ENTRYPOINT ["vector", "--config", "/etc/vector/vector.yaml"]
