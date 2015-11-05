project(test_client)

SET(SRCS
test_client.cpp
)

add_executable(test_client ${SRCS})
target_link_libraries(test_client csi-phoebe ${EXT_LIBS})
