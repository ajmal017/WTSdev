docker run \
    -p 80:80 \
    -e 'PGADMIN_DEFAULT_EMAIL=mailtocap@gmail.com' \
    -e 'PGADMIN_DEFAULT_PASSWORD=SuperSecret123$' \
    --name devpgadmin \
    -d dpage/pgadmin4
    
