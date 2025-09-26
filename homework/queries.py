"""Taller evaluable"""

# pylint: disable=broad-exception-raised
# pylint: disable=import-error

from homework.mapreduce import hadoop

#
# Columns:
# total_bill, tip, sex, smoker, day, time, size
#

#cómo codificamos un mapper y un reducer

# CONSULTA 1:
# SELECT *, tip/total_bill as tip_rate
# FROM tips;
#
#CONSULTA 2:
#SELECT *
#FROM tips
#WHERE time = 'Dinner';

#CONSULTA 3:
#SELECT *
#FROM tips
#WHERE time = 'Dinner' AND tip> 5;

#CONSULTA 4:
# SELECT *
# FROM tips
# WHERE size >= 5 OR total_bill > 45;


#CONSULTA 5:
#SELECT *
#FROM tips
# GROUP BY sex:

#----------------------------------------------------------------
def mapper_query_1(sequence): #nombres de las columnas, cuando no sea la primera columna
    """Mapper 1"""
    result = []
    for index, (_,row) in enumerate(sequence): # (_, row) porque no me interesa el nombre del archivo
        if index == 0:
            result.append(
                (
                    index, 
                    row.strip() + ",tip_rate") #añado la nueva columna, strip para quitar el salto de línea
            ) 
        else:
            columns = row.strip().split(",") #separo las columnas y quita el salto de línea
            total_bill = float(columns[0]) #primera columna
            tip = float(columns[1]) #segunda columna
            tip_rate = tip / total_bill if total_bill != 0 else 0 #evito división por cero
            result.append(
                (
                    index, 
                    row.strip() + "," + str(tip_rate) #añado la nueva columna con dos decimales
                )
            )
    return result

def reducer_query_1(sequence):
    return sequence

#----------------------------------------------------------------
def mapper_query_2(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            if row_values[5] == "Dinner":
                result.append((index, row.strip()))
    return result

def reducer_query_2(sequence):
    """Reducer"""
    return sequence

#----------------------------------------------------------------
def mapper_query_3(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            if row_values[5] == "Dinner" and float(row_values[1]) > 5.00:
                result.append((index, row.strip()))
    return result


def reducer_query_3(sequence):
    """Reducer"""
    return sequence

#----------------------------------------------------------------

def mapper_query_4(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            if int(row_values[6]) >= 5 or float(row_values[0]) > 45:
                result.append((index, row.strip()))
    return result


def reducer_query_4(sequence):
    """Reducer"""
    return sequence

#----------------------------------------------------------------
def mapper_query_5(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            continue
        row_values = row.strip().split(",")
        result.append((row_values[2], 1))
    return result


def reducer_query_5(sequence):
    """Reducer"""
    counter = dict()
    for key, value in sequence:
        if key not in counter:
            counter[key] = 0
        counter[key] += value
    return list(counter.items())

#----------------------------------------------------------------
# ORQUESTADOR:
#
def run():
    """Orquestador"""

    hadoop(
        mapper_fn=mapper_query_1,
        reducer_fn=reducer_query_1,
        input_folder="files/input",
        output_folder="files/query_1",
    )

    hadoop(
        mapper_fn=mapper_query_2,
        reducer_fn=reducer_query_2,
        input_folder="files/input",
        output_folder="files/query_2",
    )    

    hadoop(
        mapper_fn=mapper_query_3,
        reducer_fn=reducer_query_3,
        input_folder="files/input",
        output_folder="files/query_3",
    )        

    hadoop(
        mapper_fn=mapper_query_4,
        reducer_fn=reducer_query_4,
        input_folder="files/input",
        output_folder="files/query_4",
    )       

    hadoop( 
        mapper_fn=mapper_query_5,
        reducer_fn=reducer_query_5,
        input_folder="files/input",
        output_folder="files/query_5",
    )  
 


if __name__ == "__main__":

    run()



