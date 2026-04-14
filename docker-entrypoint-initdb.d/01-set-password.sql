-- Гарантируем пароль postgres при любом пересоздании контейнера
ALTER USER postgres WITH PASSWORD 'postgres';
