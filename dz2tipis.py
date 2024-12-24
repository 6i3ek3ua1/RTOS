import random
import time
from threading import Timer
import logging
import matplotlib.pyplot as plt


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
        # Генерирует 10000 случайных задач с разными параметрами
        for i in range(10000):
            priority = random.randint(1, 200)  # Случайный приоритет задачи
            operations = random.randint(10 ** 2, 10 ** 3)  # Случайное количество операций
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
        self.time_quantum = 750
        self.completed_tasks = 0  # Счётчик выполненных задач

    def execute_task(self, task, scheduler):
        # Начинаем выполнение задачи
        self.current_task = task
        self.current_task.status = "Executing"  # Изменяем статус задачи на "Выполняется"
        logging.info(f"Core-{self.core_id}: Executing task-{task.task_id}")
        # Запускаем таймер, который запустит функцию завершения выполнения задачи через заданное время
        if self.current_task.operations > self.time_quantum:
            t = Timer((self.time_quantum / (2.5 * 10 ** 9)), self.put_task_to_queue, args=(self.time_quantum, scheduler))
            t.start()
        else:
            t = Timer(task.exec_time, self.finish_task, args=(task.exec_time,))
            t.start()

    def finish_task(self, exec_time):
        # Завершаем выполнение задачи
        logging.info(
            f"Core-{self.core_id} in Processor-{self.processor_id}: Finished task-{self.current_task.task_id} in {exec_time} sec"
        )
        self.current_task.status = "Finished"  # Изменяем статус задачи на "Завершено"
        self.current_task = None  # Сбрасываем текущую задачу
        self.completed_tasks += 1  # Увеличиваем счётчик выполненных задач

    def put_task_to_queue(self, time_quantum, scheduler):
        self.current_task.operations -= time_quantum
        self.current_task.status = "Ready"
        self.current_task.priority = 127
        self.current_task.get_execution_time()
        logging.info(f"Task-{self.current_task.task_id} get back to queue")
        scheduler.queue_tasks.append(self.current_task)
        self.current_task = None


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
class PacketFrame:  # Класс, являющийся моделью кадра по стандарту IEEE P802.3ba™/D0.9
    def __init__(self, max_size: int = 12144, headers_size: int = 144, min_size: int = 512):  # Максимальные размеры кадра и размер заголовков из стандарта
        self.max_size = max_size
        self.headers_size = headers_size
        self.payload_max_size = max_size - headers_size  # В соответствии с логикой деления на кадры, задаем максимальный размер посылки
        self.payload = []  # Храним посылку, это список задач.
        self.min_size = min_size

    @staticmethod
    def count_payload(payload: list) -> int:  # Метод, считающий текущий размер посылки
        payload_size = 0
        for task in payload:
            payload_size = payload_size + task.data_size
        return payload_size

    def add_payload(self, payload: Task) -> bool:  # Добавляем задачу в посылку только если вместе с ней размер не перевалит за максимальное
        if PacketFrame.count_payload(self.payload) + payload.data_size < self.payload_max_size:
            self.payload.append(payload)
            return True  # Возвращаем True если добавили
        return False  # иначе, False.

    def get_size(self) -> int:
        if PacketFrame.count_payload(self.payload) + self.headers_size < self.min_size:
            return self.min_size
        return PacketFrame.count_payload(self.payload) + self.headers_size

    def __str__(self):  # Для удобства вывода объектов класса
        tasks_text = ""
        for task in self.payload:
            tasks_text += f"{task.task_id}: {task.size_bits}; "
        output = "Кадр {" + tasks_text + "} " + PacketFrame.count_payload(self.payload).__str__()
        return output


class NetworkTransmission:  # Класс, являющийся моделью канала связи по стандарту IEEE P802.3ba™/D0.9
    @staticmethod
    def distribute_tasks(tasks: list) -> list:  # Распределяем задачи по кадрам в соответствии с описанным принципом
        frames = []
        this_frame = PacketFrame()
        for task in tasks:
            if not this_frame.add_payload(task):  # Если получаем False при добавлении в посылку, добавляем этот кадр в список кадров и добавляем задачу к новому.
                frames.append(this_frame)
                this_frame = PacketFrame()
                this_frame.add_payload(task)
        frames.append(this_frame)
        return frames

    def __init__(self, tasks: list, data_rate: int = 100 * 10**9):
        self.frames = NetworkTransmission.distribute_tasks(tasks)  # Распределяем задачи по кадрам
        self.data_rate = data_rate  # Используем скорость передачи из стандарта

    def count_transmission_time(self) -> float:
        total_bits_size = 0
        for frame in self.frames:
            total_bits_size += frame.get_size()  # Подсчитываем в сумму размер каждого из кадров из разделения
        return total_bits_size / self.data_rate  # и возвращаем время передачи путём деления на скорость передачи

    def __str__(self):  # Для удобства вывода объектов класса
        output = ""
        for i in range(len(self.frames)):
            output += f"{i + 1}: {self.frames[i]};\n"
        return output


# Класс Scheduler управляет процессом планирования задач и их выполнения
class Scheduler:
    def __init__(self, processors, memory_map: MemoryMap):
        self.processors = processors
        self.memory_map = memory_map
        self.transmission = NetworkTransmission(memory_map.get_tasks())
        self.time_transmission = self.transmission.count_transmission_time()
        self.count_cycle = 0
        self.queue_tasks = []

    def system_process(self):
        logging.info(f"Tasks transmitted in {self.time_transmission}")
        self.queue_tasks = self.memory_map.get_tasks()
        time.sleep(self.time_transmission)
        logging.info("Start task execution")

        while len(self.queue_tasks) > 0:
            self.count_cycle += 1
            self.queue_tasks.sort(key=lambda x: x.priority)
            for task in self.queue_tasks[:]:  # Проходим по копии списка для безопасного изменения очереди
                for processor in self.processors:
                    if processor.get_free_core() is not None and task.status == "Ready":
                        core = processor.get_free_core()
                        logging.info(
                            f"Processor {processor.processor_id} delegated task {task.task_id} to core {core.core_id}")
                        core.execute_task(task, self)
                        self.queue_tasks.remove(task)
                        break
            time.sleep(self.count_cycle / (2.5 * 10 ** 9))

        print(f"Execution time - {self.count_cycle / (2.5 * 10 ** 9)}")

    def generate_report(self):
        """Генерирует графики после выполнения задач."""
        core_data = []
        task_counts_per_core = []

        processor_data = []
        task_counts_per_processor = []

        for processor in self.processors:
            total_tasks_processor = 0
            for core in processor.cores:
                core_data.append(f"Processor {processor.processor_id}, Core {core.core_id}")
                task_counts_per_core.append(core.completed_tasks)
                total_tasks_processor += core.completed_tasks
            processor_data.append(f"Processor {processor.processor_id}")
            task_counts_per_processor.append(total_tasks_processor)

        # Диаграмма распределения задач по ядрам
        plt.figure(figsize=(10, 6))
        plt.bar(core_data, task_counts_per_core, color='skyblue')
        plt.xlabel("Core")
        plt.ylabel("Number of Tasks Completed")
        plt.title("Task Distribution Across Cores")
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig("tasks_distribution_cores.png")  # Сохранение графика в файл
        print("График распределения задач по ядрам сохранён как 'tasks_distribution_cores.png'")

        # Диаграмма распределения задач по процессорам
        plt.figure(figsize=(8, 5))
        plt.bar(processor_data, task_counts_per_processor, color='lightgreen')
        plt.xlabel("Processor")
        plt.ylabel("Number of Tasks Completed")
        plt.title("Task Distribution Across Processors")
        plt.tight_layout()
        plt.savefig("tasks_distribution_processors.png")  # Сохранение графика в файл
        print("График распределения задач по процессорам сохранён как 'tasks_distribution_processors.png'")


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
    time.sleep(1)
    scheduler.generate_report()
