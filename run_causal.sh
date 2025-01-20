
# NUM_NODES=10
#python -m cs4545.system.util compose 10 topologies/dolev.yaml dolev

#export $NUM_NODES=10
python -m cs4545.system.util compose_causal 6 2 3 0 3
python -m cs4545.system.util d3 topologies/causal.yaml

# # Exit if the above command fails
# if [ $? -ne 0 ]; then
#     exit 1
# fi

docker compose build
docker compose up