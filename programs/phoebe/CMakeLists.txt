project(phoebe)

SET(SRCS
phoebe.cpp
)

add_executable(phoebe ${SRCS})
target_link_libraries(phoebe ${EXT_LIBS})
