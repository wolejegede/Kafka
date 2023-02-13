{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "48cb6990",
   "metadata": {},
   "outputs": [],
   "source": [
    "import findspark"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "b1b93a6f",
   "metadata": {},
   "outputs": [],
   "source": [
    "findspark.init('spark-3.0.1-bin-hadoop2.7')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "id": "c5a009da",
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark import SparkContext"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "id": "47987c96",
   "metadata": {},
   "outputs": [],
   "source": [
    "from pyspark.streaming import StreamingContext"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "id": "44c5a983",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "22/01/05 16:03:21 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable\n",
      "Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties\n",
      "Setting default log level to \"WARN\".\n",
      "To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).\n"
     ]
    }
   ],
   "source": [
    "sc = SparkContext('local[2]','NetworkWordCount')"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "id": "380dff7e",
   "metadata": {},
   "outputs": [],
   "source": [
    "ssc = StreamingContext(sc,1)\n",
    "#here is streamingcontext using 1 sec"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "id": "84f86fe9",
   "metadata": {},
   "outputs": [],
   "source": [
    "lines = ssc.socketTextStream('localhost',9999)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "id": "b8964323",
   "metadata": {},
   "outputs": [],
   "source": [
    "words = lines.flatMap(lambda line:line.split(' '))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "id": "df8f2c5f",
   "metadata": {},
   "outputs": [],
   "source": [
    "pairs = words.map(lambda word:(word,1))"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 11,
   "id": "a602b597",
   "metadata": {},
   "outputs": [],
   "source": [
    "words_count = pairs.reduceByKey(lambda num1,num2:num1+num2)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 12,
   "id": "c7173129",
   "metadata": {},
   "outputs": [],
   "source": [
    "words_count.pprint()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 13,
   "id": "84a592da",
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:08\n",
      "-------------------------------------------\n",
      "\n",
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:09\n",
      "-------------------------------------------\n",
      "\n",
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:10\n",
      "-------------------------------------------\n",
      "\n",
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:11\n",
      "-------------------------------------------\n",
      "\n",
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:12\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:13\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:14\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:15\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:16\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:17\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "\r",
      "[Stage 0:>                                                          (0 + 1) / 1]\r",
      "\r",
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:18\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:19\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:20\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:21\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:22\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:23\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "22/01/05 16:30:24 WARN RandomBlockReplicationPolicy: Expecting 1 replicas with only 0 peer/s.\n",
      "22/01/05 16:30:24 WARN BlockManager: Block input-0-1641380424000 replicated to only 0 peer(s) instead of 1 peers\n"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:24\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:25\n",
      "-------------------------------------------\n",
      "('hello', 1)\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:26\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:27\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:28\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "22/01/05 16:30:28 WARN RandomBlockReplicationPolicy: Expecting 1 replicas with only 0 peer/s.\n",
      "22/01/05 16:30:28 WARN BlockManager: Block input-0-1641380428200 replicated to only 0 peer(s) instead of 1 peers\n",
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:29\n",
      "-------------------------------------------\n",
      "('hello', 1)\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:30\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:31\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:32\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:33\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:34\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:35\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "22/01/05 16:30:35 WARN RandomBlockReplicationPolicy: Expecting 1 replicas with only 0 peer/s.\n",
      "22/01/05 16:30:35 WARN BlockManager: Block input-0-1641380435400 replicated to only 0 peer(s) instead of 1 peers\n",
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:36\n",
      "-------------------------------------------\n",
      "('world', 1)\n",
      "('hello', 1)\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:37\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:38\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "\r",
      "[Stage 0:>                                                          (0 + 1) / 1]\r",
      "\r",
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:39\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:40\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:41\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:42\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:43\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:44\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:45\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:46\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:47\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:48\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:49\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:50\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:51\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:52\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:53\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:54\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:55\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:56\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:57\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:58\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:30:59\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:00\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:01\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:02\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:03\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:04\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:05\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:06\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:07\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:08\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:09\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:10\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:11\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:12\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:13\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:14\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "22/01/05 16:31:15 WARN RandomBlockReplicationPolicy: Expecting 1 replicas with only 0 peer/s.\n",
      "22/01/05 16:31:15 WARN BlockManager: Block input-0-1641380475000 replicated to only 0 peer(s) instead of 1 peers\n"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:15\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:16\n",
      "-------------------------------------------\n",
      "('this', 1)\n",
      "('is', 1)\n",
      "('line', 1)\n",
      "('multiple', 1)\n",
      "('a', 1)\n",
      "('with', 1)\n",
      "('words', 1)\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:17\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:18\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:19\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:20\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:21\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:22\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:23\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:24\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:25\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:26\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:27\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:28\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:29\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:30\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:31\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:32\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:33\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:34\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:35\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:36\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:37\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:38\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:39\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:40\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:41\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:42\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:43\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:44\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:45\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:46\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:47\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:48\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:49\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:50\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:51\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:52\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:53\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:54\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:55\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:56\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:57\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:58\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "\r",
      "[Stage 0:>                                                          (0 + 1) / 1]\r",
      "\r",
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:31:59\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:32:00\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:32:01\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:32:02\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:32:03\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:32:04\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:32:05\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:32:06\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:32:07\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:32:08\n",
      "-------------------------------------------\n",
      "\n"
     ]
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "                                                                                \r"
     ]
    },
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "-------------------------------------------\n",
      "Time: 2022-01-05 16:32:09\n",
      "-------------------------------------------\n",
      "\n"
     ]
    }
   ],
   "source": [
    "ssc.start()"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.8.12"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
