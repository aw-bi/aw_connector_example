from pathlib import Path

import aiofiles
import aiofiles.os


class ParquetQueue:
    """ """
    def __init__(self, root: Path):
        self.root = root

    async def start_task(self, task_id: str):
        """ 
        """
        if await aiofiles.os.path.exists(self.root / task_id):
            # задача уже стартовала, ничего не делаем
            return
        
        task_folder = self.root / task_id
        await aiofiles.os.makedirs(task_folder)
        async with aiofiles.open(task_folder / 'status', mode='w') as f:
            await f.write('started')

        return
    
    async def finish_task(self, task_id: str):
        """ 
        """
        task_folder = self.root / task_id
        
        if await aiofiles.os.path.exists(task_folder):
            async with aiofiles.open(task_folder / 'status', mode='w') as f:
                await f.write('finished')

    async def error_task(self, task_id: str, error: str):
        task_folder = self.root / task_id
        
        if await aiofiles.os.path.exists(task_folder):
            async with aiofiles.open(task_folder / 'status', mode='w') as f:
                await f.write(f'error: {error}')

    async def clear_task(self, task_id: str):
        """ 
        """
        task_folder = self.root / task_id
        task_status_file = task_folder / 'status'
        if await aiofiles.os.path.exists(task_status_file):
            await aiofiles.os.remove(task_status_file)

        if await aiofiles.os.path.exists(task_folder):
            await aiofiles.os.rmdir(task_folder)

    async def get_task_status(self, task_id):
        """ 
        """
        task_status_file = self.root / task_id / 'status'
        if await aiofiles.os.path.exists(task_status_file):
            async with aiofiles.open(task_status_file, mode='r') as f:
                return await f.read()
        return 'not found'
