
# NUM_NODES=10
#python -m cs4545.system.util compose 10 topologies/dolev.yaml dolev

#export $NUM_NODES=10
python -m cs4545.system.util compose_bracha 5 1 1 1 4 IGNORE_MSG
python -m cs4545.system.util d2 topologies/bracha.yaml

# # Exit if the above command fails
# if [ $? -ne 0 ]; then
#     exit 1
# fi

docker compose build
docker compose up