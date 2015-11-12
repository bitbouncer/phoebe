project(kafka-rm-schema)

SET(SRCS
kafka-rm-schema.cpp
)

add_executable(kafka-rm-schema ${SRCS})
target_link_libraries(kafka-rm-schema ${EXT_LIBS})
