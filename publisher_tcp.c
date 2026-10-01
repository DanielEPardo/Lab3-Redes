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

void publicar_evento(int socket_cliente, const char *tema, const char *mensaje) {
    char buffer_envio[TAM_BUFFER];
    /* El '\n' final delimita el mensaje: sin él TCP podría pegarlo con el siguiente */
    snprintf(buffer_envio, sizeof(buffer_envio), "PUB %s %s\n", tema, mensaje);

    if (send(socket_cliente, buffer_envio, strlen(buffer_envio), 0) < 0)
        perror("Error al enviar el evento de publicación");
    else
        printf("[PUB] Evento publicado en '%s': %s\n", tema, mensaje);
}

int main(int argc, char *argv[]) {

    /* Crear el socket TCP del publicador */
    int socket_cliente = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_cliente < 0) {
        perror("Error al crear el socket del publicador");
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

    /* Publicar eventos */
    // Opción A: Modo automático con N eventos numerados en argumento de consola
    if (argc >= 4 && strcmp(argv[1], "auto") == 0) {
        int cantidad = atoi(argv[3]);
        int pausa_ms = (argc >= 5) ? atoi(argv[4]) : 500; 
        char mensaje[TAM_BUFFER];
        for (int i = 1; i <= cantidad; i++) {
            snprintf(mensaje, sizeof(mensaje), "Evento %d de %d", i, cantidad);
            publicar_evento(socket_cliente, argv[2], mensaje);
            if (pausa_ms > 0) usleep(pausa_ms * 1000);
        }
    }
    // Opción B: Modo manual con tema y mensaje en argumentos de consola
    else if (argc >= 3) {
        char mensaje[TAM_BUFFER] = "";
        for (int i = 2; i < argc; i++) {
            size_t usado = strlen(mensaje);
            snprintf(mensaje + usado, sizeof(mensaje) - usado, "%s%s", (i > 2) ? " " : "", argv[i]);
        }
        publicar_evento(socket_cliente, argv[1], mensaje);
    }
    // Opción C: Modo interactivo continuo por consola
    else {
        printf("=== MODO INTERACTIVO === (escriba 'salir' como partido para terminar)\n");
        char tema[MAX_TEMA], mensaje[TAM_BUFFER];
        while (1) {
            printf("Nombre del partido: ");
            if (scanf("%63s", tema) != 1) 
                break;
            if (strcmp(tema, "SALIR") == 0 || strcmp(tema, "salir") == 0) 
                break;
            getchar();   /* descarta el '\n' que dejó scanf */
            printf("Mensaje del evento: ");
            if (fgets(mensaje, sizeof(mensaje), stdin) != NULL) {
                mensaje[strcspn(mensaje, "\n")] = '\0';
                publicar_evento(socket_cliente, tema, mensaje);
            }
        }
    }

    close(socket_cliente);
    printf("[-] Desconectado del Broker.\n");
    return 0;
}