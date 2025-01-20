
# NUM_NODES=10
#python -m cs4545.system.util compose 10 topologies/dolev.yaml dolev

#export $NUM_NODES=10
python -m cs4545.system.util compose_dolev 5 1 1 1 3 MODIFY_MSG_ID
python -m cs4545.system.util compose_dolev 10 2 2 2 5 
python -m cs4545.system.util d2 topologies/dolev.yaml

# # Exit if the above command fails
# if [ $? -ne 0 ]; then
#     exit 1
# fi

docker compose build
docker compose up