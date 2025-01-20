import random
import time
from threading import Timer
import logging
import matplotlib.pyplot as plt
import scipy.stats as stats

# Настройка логгирования для записи действий программы в файл
logging.basicConfig(
    filename="task_scheduler.log",  # Имя файла для сохранения логов
    level=logging.INFO,  # Уровень логирования (информационные сообщения)
    format="%(asctime)s - %(levelname)s - %(message)s"  # Формат записей логов
)

TASK_COUNTER = 0  # Глобальный счетчик выполненных задач


# Класс Task представляет задачу с её основными характеристиками
class Task:
    def __init__(self, task_id, data_size, priority, operations):
        self.task_id = task_id  # Уникальный идентификатор задачи
        self.size_bits = data_size  # Размер данных задачи в битах
        self.priority = priority  # Приоритет задачи (чем меньше число, тем выше приоритет)
        self.status = "Ready"  # Начальный статус задачи — "Готова к выполнению"
        self.operations = operations  # Количество операций для выполнения задачи
        self.exec_time = 0  # Время выполнения задачи в секундах
        self.get_execution_time()  # Расчёт времени выполнения задачи

    def get_execution_time(self):
        # Расчёт времени выполнения задачи по формуле: операции делятся на производительность системы
        self.exec_time = self.operations / (1 * 10 ** 9)


# Класс MemoryMap управляет задачами, находящимися в памяти
class MemoryMap:
    def __init__(self):
        self.tasks = []  # Список задач, добавленных в память
        self.count_tasks = 0  # Общее количество задач

    def add_task(self, task):
        # Добавление задачи в список
        self.tasks.append(task)

    def get_tasks(self):
        # Возвращает список задач, отсортированный по приоритету
        self.tasks.sort(key=lambda x: x.priority)
        return self.tasks

    def generate_tasks(self, count_tasks):
        # Генерация случайных задач с определёнными характеристиками
        self.count_tasks = count_tasks
        for i in range(count_tasks):
            priority = random.randint(1, 200)  # Случайный приоритет
            operations = random.randint(10 ** 2, 10 ** 3)  # Количество операций
            data_size = random.randint(128, 12145)  # Размер данных
            task = Task(i, data_size, priority, operations)  # Создание объекта задачи
            self.add_task(task)  # Добавление задачи в память
            logging.info(
                f"Task-{task.task_id}, Size: {task.size_bits}, Priority: {task.priority}, Operations: {task.operations})"
            )


# Класс Core представляет ядро процессора, которое выполняет задачи
class Core:
    def __init__(self, core_id, processor_id):
        self.core_id = core_id  # Идентификатор ядра
        self.current_task = None  # Текущая выполняемая задача (или None, если ядро свободно)
        self.processor_id = processor_id  # Идентификатор процессора, которому принадлежит ядро
        self.time_quantum = 750  # Квант времени для обработки задачи
        self.completed_tasks = 0  # Счётчик выполненных задач ядром

    def execute_task(self, task, scheduler):
        # Начало выполнения задачи
        self.current_task = task
        self.current_task.status = "Executing"  # Установка статуса задачи как "Выполняется"
        logging.info(f"Core-{self.core_id}: Executing task-{task.task_id}")
        # Настройка таймера для завершения или возврата задачи в очередь
        if self.current_task.operations > self.time_quantum:
            t = Timer((self.time_quantum / (1 * 10 ** 9)), self.put_task_to_queue, args=(self.time_quantum, scheduler))
            t.start()
        else:
            t = Timer(task.exec_time, self.finish_task, args=(task.exec_time, scheduler))
            t.start()

    def finish_task(self, exec_time, scheduler):
        global TASK_COUNTER
        # Завершение выполнения задачи
        logging.info(
            f"Core-{self.core_id} in Processor-{self.processor_id}: Finished task-{self.current_task.task_id} in {exec_time} sec"
        )
        scheduler.active_tasks.remove(self.current_task)  # Удаление задачи из списка активных
        self.current_task.status = "Finished"  # Установка статуса задачи как "Завершена"
        self.current_task = None  # Очистка текущей задачи
        self.completed_tasks += 1  # Увеличение счётчика завершённых задач
        TASK_COUNTER += 1  # Увеличение глобального счётчика задач

    def put_task_to_queue(self, time_quantum, scheduler):
        # Возвращение задачи в очередь с уменьшением операций
        self.current_task.operations -= time_quantum
        self.current_task.status = "Ready"
        self.current_task.priority = 127  # Установка нового приоритета
        self.current_task.get_execution_time()  # Пересчёт времени выполнения
        logging.info(f"Task-{self.current_task.task_id} get back to queue")
        scheduler.active_tasks.remove(self.current_task)  # Удаление из активных
        scheduler.queue_tasks.append(self.current_task)  # Добавление обратно в очередь
        self.current_task = None  # Освобождение ядра


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
    def __init__(self, max_size: int = 12144, headers_size: int = 144, min_size: int = 512):
        # Задаем параметры кадра: максимальный размер, размер заголовков и минимальный размер
        self.max_size = max_size
        self.headers_size = headers_size
        self.payload_max_size = max_size - headers_size  # Вычисляем максимальный размер полезной нагрузки
        self.payload = []  # Инициализируем список задач в полезной нагрузке
        self.min_size = min_size

    @staticmethod
    def count_payload(payload: list) -> int:
        # Подсчитываем общий размер задач в полезной нагрузке
        payload_size = 0
        for task in payload:
            payload_size += task.size_bits  # Суммируем размер каждой задачи
        return payload_size

    def add_payload(self, payload: Task) -> bool:
        # Добавляем задачу в кадр, если общий размер с учетом новой задачи не превышает допустимый
        if PacketFrame.count_payload(self.payload) + payload.size_bits < self.payload_max_size:
            self.payload.append(payload)
            return True  # Успешно добавили задачу
        return False  # Размер превышает допустимый, задача не добавлена

    def get_size(self) -> int:
        # Возвращаем размер кадра, учитывая заголовки
        if PacketFrame.count_payload(self.payload) + self.headers_size < self.min_size:
            return self.min_size  # Если размер меньше минимального, возвращаем минимальный
        return PacketFrame.count_payload(self.payload) + self.headers_size

    def __str__(self):
        # Возвращаем строковое представление кадра с задачами и их размерами
        tasks_text = ""
        for task in self.payload:
            tasks_text += f"{task.task_id}: {task.size_bits}; "
        output = "Кадр {" + tasks_text + "} " + PacketFrame.count_payload(self.payload).__str__()
        return output


class NetworkTransmission:
    @staticmethod
    def distribute_tasks(tasks: list) -> list:
        # Распределяем задачи по кадрам, пока не достигнем максимального размера кадра
        frames = []
        this_frame = PacketFrame()
        for task in tasks:
            if not this_frame.add_payload(task):  # Если задача не помещается в текущий кадр
                frames.append(this_frame)  # Сохраняем кадр
                this_frame = PacketFrame()  # Создаем новый кадр
                this_frame.add_payload(task)  # Добавляем задачу в новый кадр
        frames.append(this_frame)  # Добавляем последний кадр
        return frames

    def __init__(self, tasks: list, data_rate: int = 100 * 10**9):
        # Инициализируем передачу данных: распределяем задачи по кадрам и задаем скорость передачи
        self.frames = NetworkTransmission.distribute_tasks(tasks)
        self.data_rate = data_rate

    def count_transmission_time(self) -> float:
        # Подсчитываем общее время передачи данных
        total_bits_size = 0
        for frame in self.frames:
            total_bits_size += frame.get_size()  # Суммируем размеры всех кадров
        return total_bits_size / self.data_rate  # Делим общий размер на скорость передачи

    def __str__(self):
        # Возвращаем строковое представление всех кадров
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
        self.active_tasks = []

    def system_process(self):
        logging.info(f"Tasks transmitted in {self.time_transmission}") # Логируем начало передачи задач
        self.queue_tasks = self.memory_map.get_tasks() # Получаем задачи из карты памяти
        self.active_tasks = [] # Список активных задач (те, которые в процессе выполнения)
        time.sleep(self.time_transmission) # Ожидаем окончания передачи задач
        logging.info("Start task execution") # Логируем начало выполнения задач
        num_proc = 0 # Номер текущего процессора

        # Цикл продолжается, пока есть задачи в очереди или активные задачи
        while len(self.queue_tasks) > 0 or len(self.active_tasks) > 0:
            self.queue_tasks.sort(key=lambda x: x.priority) # Сортируем задачи по приоритету (от наивысшего к низшему)
            # Итерируем по копии очереди задач
            for task in self.queue_tasks[:]:
                # Ограничиваем индекс процессора количеством доступных процессоров
                if num_proc >= len(self.processors):
                    num_proc -= len(self.processors)
                curr_processor = self.processors[num_proc] # Получаем текущий процессор
                # Проверяем доступность ядра у текущего процессора
                if curr_processor.get_free_core is None:
                    # Если нет доступного ядра, ищем процессор с доступным ядром
                    for processor in self.processors:
                        num_proc += 1
                        if processor.get_free_core():
                            curr_processor = processor
                # Если найдено доступное ядро и задача готова к выполнению
                if curr_processor.get_free_core() is not None and task.status == "Ready":
                    core = curr_processor.get_free_core()
                    # Логируем делегирование задачи ядру
                    logging.info(
                        f"Processor {curr_processor.processor_id} delegated task {task.task_id} to core {core.core_id}")
                    core.execute_task(task, self) # Передаем задачу на выполнение
                    # Добавляем задачу в список активных и удаляем из очереди
                    self.active_tasks.append(task)
                    self.queue_tasks.remove(task)
                # Переходим к следующему процессору
                num_proc += 1
                self.count_cycle += 1 # Увеличиваем счетчик циклов
                time.sleep(1 / (1 * 10 ** 9)) # Имитируем задержку выполнения

        # Выводим общее время работы
        print(f"Общее время работы - {(self.count_cycle / (1 * 10 ** 9)) + self.time_transmission}")

    def generate_report(self):
        """Генерирует графики и выводит статистику после выполнения задач."""
        core_labels = []  # Метки для ядер
        task_counts_per_core = []  # Количество задач на каждом ядре

        processor_labels = []  # Метки для процессоров
        task_counts_per_processor = []  # Количество задач на каждом процессоре

        # Собираем данные о задачах, выполненных ядрами и процессорами
        for processor in self.processors:
            total_tasks_processor = 0  # Общее количество задач для процессора
            for core in processor.cores:
                core_labels.append(f"P{processor.processor_id}-C{core.core_id}")
                task_counts_per_core.append(core.completed_tasks)
                total_tasks_processor += core.completed_tasks
            processor_labels.append(f"Processor {processor.processor_id}")
            task_counts_per_processor.append(total_tasks_processor)

        # Вычисление статистики для процессоров
        processor_mean = sum(task_counts_per_processor) / len(task_counts_per_processor)
        processor_variance = sum((x - processor_mean) ** 2 for x in task_counts_per_processor) / len(
            task_counts_per_processor)
        processor_std_dev = processor_variance ** 0.5
        processor_variation_coeff = processor_std_dev / processor_mean if processor_mean != 0 else 0

        # Вывод статистики
        print("Processor Statistics:")
        print(f"  Среднее значение выполненных задач: {processor_mean:.2f}")
        print(f"  Среднее квадратичное отклонение: {processor_variance:.2f}")
        print(f"  Дисперсия: {processor_std_dev:.2f}")
        print(f"  Коэффициент вариации: {processor_variation_coeff:.2%}")

        # Построение графика для ядер
        plt.figure(figsize=(10, 6))
        plt.bar(core_labels, task_counts_per_core, color='skyblue')
        plt.xlabel("Cores")
        plt.ylabel("Number of Tasks Completed")
        plt.title("Task Distribution Across Cores")
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig("tasks_distribution_cores.png")  # Сохранение графика
        print("График распределения задач по ядрам сохранён как 'tasks_distribution_cores.png'")

        # Построение графика для процессоров
        plt.figure(figsize=(8, 5))
        plt.bar(processor_labels, task_counts_per_processor, color='lightgreen')
        plt.xlabel("Processors")
        plt.ylabel("Number of Tasks Completed")
        plt.title("Task Distribution Across Processors")
        plt.tight_layout()
        plt.savefig("tasks_distribution_processors.png")  # Сохранение графика
        print("График распределения задач по процессорам сохранён как 'tasks_distribution_processors.png'")

        # Анализ заполненности кадров Ethernet
        frames_occupancy = []  # Заполняемость каждого кадра
        for frame in self.transmission.frames:
            total_frame_payload = frame.count_payload(frame.payload)
            frames_occupancy.append((total_frame_payload / 12000) * 100)
        indicies = list(range(len(frames_occupancy)))  # Индексы кадров
        avg_occupancy = sum(frames_occupancy) / len(frames_occupancy)  # Средняя заполненность
        plt.figure(figsize=(10, 6))
        plt.bar(indicies, frames_occupancy, color='blue', width=1)
        plt.axhline(avg_occupancy, color='red', linestyle='--', linewidth=2, label="Средняя заполненность")
        plt.title("Заполненность кадров ethernet в процентах")
        plt.xlabel("Номер кадра")
        plt.ylabel("Заполненность")
        plt.tight_layout()
        plt.savefig("frame_utilisation_plot.png")  # Сохранение графика
        print(f"Средняя заполненность кадров: {avg_occupancy}%")

        # Проверка равномерности распределения задач с помощью критерия хи-квадрат
        all_tasks_count = self.memory_map.count_tasks
        expected_tasks_per_processor = all_tasks_count / 8  # Ожидаемое распределение
        observed_counts = task_counts_per_processor
        expected_counts = [expected_tasks_per_processor] * len(self.processors)

        chi_square_statistic, p_value = stats.chisquare(observed_counts, expected_counts) # Вычисление статистики хи-квадрат

        # Вывод результатов
        print(f"Критерий хи-квадрат: {chi_square_statistic:.2f}")
        print(f"p-значение: {p_value:.4f}")
        if p_value < 0.05:
            print("Отвергаем нулевую гипотезу: задачи распределены неравномерно между процессорами.")
        else:
            print("Не отвергаем нулевую гипотезу: задачи распределены равномерно между процессорами.")


# Основной блок программы, где происходит создание объектов и запуск планировщика
if __name__ == "__main__":
    memory_map = MemoryMap()  # Создаем карту памяти
    count_tasks = int(input("Введите количество задач "))
    print(f"Будут сгенерированы {count_tasks} задач со случайными параметрами:\n"
          f"размер в операциях\n"
          f"приоритет\n"
          f"размер данных\n")
    memory_map.generate_tasks(count_tasks)  # Генерируем задачи
    processors = []

    # Создаем три процессора, каждый с 3 ядрами
    for i in range(8):
        processors.append(Processor(i, 8))

    scheduler = Scheduler(processors, memory_map)  # Создаем планировщик
    scheduler.system_process()  # Запускаем процесс выполнения задач
    time.sleep(1)
    scheduler.generate_report()
    print(f"Количество выполненных задач - {TASK_COUNTER}\n"
          f"Процесс выполнения задач системой отражён в файле task_scheduler.log")
