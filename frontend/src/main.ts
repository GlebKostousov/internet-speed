import { mount } from 'svelte';
import App from './App.svelte';
import './app.css';

const target = document.getElementById('app');
if (target === null) throw new Error('Не найден корневой элемент интерфейса.');
mount(App, { target });
