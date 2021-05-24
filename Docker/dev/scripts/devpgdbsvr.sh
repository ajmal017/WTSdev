docker run -d \
	--name devpgdbsvr \
	-e POSTGRES_PASSWORD=Pas2021! \
	-v /home/wts/dev/postgresqldata:/var/lib/postgresql/data \
        -p 5432:5432 \
        postgres
# docker inspect pdgbsvr -f "{{json .NetworkSettings.Networks }}"
