project(avrofile2postgres)

if(WIN32)
    SET (PGSQLSRC "C:/Program Files/PostgreSQL/9.5")
    include_directories(${PGSQLSRC}/include)
    include_directories(${PGSQLSRC}/include/server)
    link_directories(${CMAKE_SOURCE_DIR}/lib/$(Platform))
    link_directories("C:/Program Files/PostgreSQL/9.5/lib")
    set(EXT_LIBS postgres_asio libpq ${EXT_LIBS}) 
endif()    

if(__LINUX__)
     set(EXT_LIBS postgres_asio pq ${EXT_LIBS})
endif()

SET(SRCS
avro_postgres.cpp
avro_postgres.h
avrofile2postgres.cpp
)

add_executable(avrofile2postgres ${SRCS})
target_link_libraries(avrofile2postgres ${EXT_LIBS})
