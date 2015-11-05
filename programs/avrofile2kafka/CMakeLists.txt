project(avrofile2kafka)

SET(SRCS
avrofile2kafka.cpp
)

add_executable(avrofile2kafka ${SRCS})
target_link_libraries(avrofile2kafka ${EXT_LIBS})
