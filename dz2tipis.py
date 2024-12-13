import random
import time
from threading import Timer
import logging

# Настройка логгирования
logging.basicConfig(
    filename="task_scheduler.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# Класс Task представляет задачу с уникальным идентификатором, размером данных, приоритетом и количеством операций
class Task:
    def __init__(self, task_id, data_size, priority, operations):
        self.task_id = task_id  # Уникальный идентификатор задачи
        self.data_size = data_size  # Размер данных задачи
        self.priority = priority  # Приоритет задачи
        self.status = "Ready"  # Статус задачи (по умолчанию готова)
        self.operations = operations  # Количество операций, которое задача должна выполнить
        self.exec_time = 0  # Время выполнения задачи (в секундах)
        self.get_execution_time()  # Расчет времени выполнения задачи

    def get_execution_time(self):
        # Вычисляем время выполнения задачи, исходя из количества операций
        self.exec_time = self.operations / (2.5 * 10 ** 9)


# Класс MemoryMap представляет карту памяти, которая управляет задачами
class MemoryMap:
    def __init__(self):
        self.tasks = []  # Список задач в памяти

    def add_task(self, task):
        # Добавляет задачу в список задач
        self.tasks.append(task)

    def get_tasks(self):
        # Сортирует задачи по приоритету и возвращает их
        self.tasks.sort(key=lambda x: x.priority)
        return self.tasks

    def generate_tasks(self):
        # Генерирует 15 случайных задач с разными параметрами
        for i in range(200):
            priority = random.randint(1, 200)  # Случайный приоритет задачи
            operations = random.randint(10 ** 9, 10 ** 10)  # Случайное количество операций
            data_size = random.randint(1, 128)  # Случайный размер данных задачи
            task = Task(i, data_size, priority, operations)  # Создаем задачу
            self.add_task(task)  # Добавляем задачу в карту памяти
            logging.info(
                f"Task-{task.task_id}, Size: {task.data_size}, Priority: {task.priority}, Operations: {task.operations})"
            )


# Класс Core представляет ядро процессора, которое выполняет задачи
class Core:
    def __init__(self, core_id, processor_id):
        self.core_id = core_id  # Идентификатор ядра
        self.current_task = None  # Текущая задача, которая выполняется на ядре
        self.processor_id = processor_id  # Идентификатор процессора, к которому относится ядро

    def execute_task(self, task):
        # Начинаем выполнение задачи
        self.current_task = task
        self.current_task.status = "Executing"  # Изменяем статус задачи на "Выполняется"
        logging.info(f"Core-{self.core_id}: Executing task-{task.task_id}")
        # Запускаем таймер, который запустит функцию завершения выполнения задачи через заданное время
        t = Timer(task.exec_time, self.finish_task, args=(task.exec_time,))
        t.start()

    def finish_task(self, exec_time):
        # Завершаем выполнение задачи
        logging.info(
            f"Core-{self.core_id} in Processor-{self.processor_id}: Finished task-{self.current_task.task_id} in {exec_time} sec"
        )
        self.current_task.status = "Finished"  # Изменяем статус задачи на "Завершено"
        self.current_task = None  # Сбрасываем текущую задачу


# Класс Processor представляет процессор, который состоит из нескольких ядер
class Processor:
    def __init__(self, processor_id, num_cores):
        self.processor_id = processor_id  # Идентификатор процессора
        # Создаем список ядер процессора
        self.cores = [Core(core_id=i, processor_id=self.processor_id) for i in range(num_cores)]

    def get_free_core(self):
        # Ищем свободное ядро для выполнения задачи
        for core in self.cores:
            if core.current_task is None:  # Если ядро не выполняет задачу
                return core
        return None  # Если все ядра заняты, возвращаем None


# Класс PacketFrame представляет кадр для передачи данных
class PacketFrame:
    def __init__(self, total_size=512, header_size=144):
        self.total_size = total_size  # Общий размер кадра
        self.header_size = header_size  # Размер заголовка
        self.max_payload_size = total_size - header_size  # Максимальный размер полезной нагрузки
        self.tasks = []  # Список задач, которые будут переданы в кадре

    def add_task_to_frame(self, task):
        """Добавляем задачу в кадр, если размер позволяет."""
        if sum(t.data_size for t in self.tasks) + task.data_size <= self.max_payload_size:
            # Если размер задач в кадре и новая задача укладываются в максимальный размер полезной нагрузки
            self.tasks.append(task)  # Добавляем задачу в кадр
            return True
        return False  # Если не хватает места, возвращаем False


# Класс NetworkTransmission представляет процесс передачи данных
class NetworkTransmission:
    def __init__(self, tasks, bandwidth=100 * 10 ** 9):
        self.frames = self._distribute_tasks_into_frames(tasks)  # Разбиваем задачи на кадры
        self.bandwidth = bandwidth  # Ширина канала передачи данных

    @staticmethod
    def _distribute_tasks_into_frames(tasks):
        # Разбиваем задачи на кадры
        frames = []
        current_frame = PacketFrame()  # Создаем новый кадр
        for task in tasks:
            if not current_frame.add_task_to_frame(task):  # Если задача не помещается в текущий кадр
                frames.append(current_frame)  # Добавляем текущий кадр в список кадров
                current_frame = PacketFrame()  # Создаем новый кадр
            current_frame.add_task_to_frame(task)  # Добавляем задачу в кадр
        return frames

    def calculate_transfer_time(self):
        """Вычисляет общее время передачи всех данных."""
        total_bits = sum(frame.total_size for frame in self.frames)  # Суммируем размер всех кадров
        return total_bits / self.bandwidth  # Рассчитываем время передачи в секундах


# Класс Scheduler управляет процессом планирования задач и их выполнения
class Scheduler:
    def __init__(self, processors, memory_map: MemoryMap):
        self.processors = processors  # Список процессоров
        self.memory_map = memory_map  # Карта памяти с задачами
        self.transmission = NetworkTransmission(memory_map.get_tasks())  # Создаем объект для расчета времени передачи данных
        self.time_transmission = self.transmission.calculate_transfer_time()  # Рассчитываем время передачи данных

    def system_process(self):
        start_time = time.time()  # Время начала выполнения всех задач
        logging.info(f"Tasks transmitted in {self.time_transmission}")  # Выводим время передачи данных
        tasks = self.memory_map.get_tasks()  # Получаем отсортированные задачи
        time.sleep(self.time_transmission)
        logging.info("Start task execution")  # Начинаем выполнение задач
        waiting_tasks = tasks  # Создание списка ожидающих задач (первичное заполнение для вхождения в цикл)
        while len(waiting_tasks) > 0:
            waiting_tasks = []  # Сбрасываем список ожидающих задач
            for task in tasks:
                for processor in self.processors:
                    # Пытаемся найти свободное ядро для каждой задачи
                    if processor.get_free_core() is not None and task.status == "Ready":
                        core = processor.get_free_core()  # Получаем свободное ядро
                        logging.info(f"Processor {processor.processor_id} delegated task {task.task_id} to core {core.core_id}")  # Выводим информацию
                        core.execute_task(task)  # Запускаем выполнение задачи
                if task.status == "Ready":  # Если задача еще не была выполнена
                    waiting_tasks.append(task)  # Добавляем её в список ожидающих
            if len(waiting_tasks) != 0:  # Если остались задачи, которые нужно выполнить
                tasks = waiting_tasks  # Обновляем список задач
                for task in tasks:
                    logging.info(f"Task-{task.task_id} in waiting_mode")  # Выводим информацию о задачах, которые в ожидании
                time.sleep(1)  # Задержка перед повторной проверкой
        end_time = time.time()  # Время окончания выполнения всех задач
        total_execution_time = end_time - start_time  # Общее время выполнения
        logging.info(f"TOTAL EXECUTION TIME: {total_execution_time} seconds")


# Основной блок программы, где происходит создание объектов и запуск планировщика
if __name__ == "__main__":
    memory_map = MemoryMap()  # Создаем карту памяти
    memory_map.generate_tasks()  # Генерируем задачи

    # Создаем три процессора, каждый с 3 ядрами
    processor1 = Processor(1, 3)
    processor2 = Processor(2, 3)
    processor3 = Processor(3, 3)

    processors = [processor1, processor2, processor3]  # Список процессоров

    scheduler = Scheduler(processors, memory_map)  # Создаем планировщик
    scheduler.system_process()  # Запускаем процесс выполнения задач

