#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define DIRECCION_BROKER "127.0.0.1"
#define PUERTO_BROKER 7000
#define TAM_BUFFER 512
#define MAX_TEMA 64
#define MAX_TEMAS_POR_CLIENTE 10

void suscribir_a_tema(int socket_cliente, const char *tema) {
    char mensaje_registro[TAM_BUFFER];
    /* Con '\n' cada SUB es un mensaje independiente: ya no hace falta el usleep */
    snprintf(mensaje_registro, sizeof(mensaje_registro), "SUB %s\n", tema);

    if (send(socket_cliente, mensaje_registro, strlen(mensaje_registro), 0) < 0)
        perror("Error al enviar la solicitud de suscripción");
    else
        printf("[SUB] Solicitud enviada para el partido: '%s'\n", tema);
}

int main(int argc, char *argv[]) {

    /* Crear el socket TCP del cliente */
    int socket_cliente = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_cliente < 0) {
        perror("Error al crear el socket del suscriptor");
        exit(EXIT_FAILURE);
    }

    /* Configurar la dirección del Broker */
    struct sockaddr_in direccion_servidor;
    memset(&direccion_servidor, 0, sizeof(direccion_servidor));
    direccion_servidor.sin_family = AF_INET;
    direccion_servidor.sin_port = htons(PUERTO_BROKER);
    if (inet_pton(AF_INET, DIRECCION_BROKER, &direccion_servidor.sin_addr) != 1) {
        fprintf(stderr, "Dirección IP del Broker inválida\n");
        close(socket_cliente);
        exit(EXIT_FAILURE);
    }

    /* Conectarse al Broker */
    if (connect(socket_cliente, (struct sockaddr *)&direccion_servidor, sizeof(direccion_servidor)) < 0) {
        perror("Error al conectar con el Broker TCP");
        close(socket_cliente);
        exit(EXIT_FAILURE);
    }
    printf("[+] Conectado al Broker TCP (%s:%d)\n", DIRECCION_BROKER, PUERTO_BROKER);

    /* Suscribirse a los temas */
    // Opción A: Pasar temas por argumento de consola
    if (argc > 1) { 
        for (int i = 1; i < argc && i <= MAX_TEMAS_POR_CLIENTE; i++)
            suscribir_a_tema(socket_cliente, argv[i]);
    }
    // Opción B: Modo interactivo continuo por consola
    else {
        printf("=== MODO INTERACTIVO === Partidos a seguir ('FIN' para terminar):\n");
        char tema[MAX_TEMA];
        int conteo = 0;
        while (conteo < MAX_TEMAS_POR_CLIENTE) {
            printf("Partido %d: ", conteo + 1);
            if (scanf("%63s", tema) != 1) break;
            if (strcmp(tema, "FIN") == 0 || strcmp(tema, "fin") == 0) break;
            suscribir_a_tema(socket_cliente, tema);
            conteo++;
        }
    }

    printf("\nEsperando notificaciones (Ctrl+C para salir)...\n\n");

    /* Acumulador: un recv puede traer medio mensaje o varios pegados */
    char acumulado[TAM_BUFFER];
    int longitud = 0;

    while (1) {
        int espacio = (int)sizeof(acumulado) - 1 - longitud;
        /* línea sin '\n' es demasiado larga: se descarta */
        if (espacio <= 0) { 
            longitud = 0; 
            continue; 
        }   

        int n = recv(socket_cliente, acumulado + longitud, espacio, 0);
        if (n == 0) { 
            printf("\n[-] El Broker se ha desconectado.\n"); 
            break; 
        }
        if (n < 0)  { 
            perror("Error al recibir datos del Broker"); 
            break; 
        }

        longitud += n;
        char *inicio = acumulado, *fin;
        while ((fin = memchr(inicio, '\n', acumulado + longitud - inicio)) != NULL) {
            *fin = '\0';
            printf("[NOTIFICACIÓN] %s\n", inicio);
            inicio = fin + 1;
        }
        int resto = acumulado + longitud - inicio;
        memmove(acumulado, inicio, resto);
        longitud = resto;
    }

    close(socket_cliente);
    return 0;
}