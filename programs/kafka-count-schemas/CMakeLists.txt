project(kafka-count-schemas)

SET(SRCS
kafka-count-schemas.cpp
)

add_executable(kafka-count-schemas ${SRCS})
target_link_libraries(kafka-count-schemas ${EXT_LIBS})
